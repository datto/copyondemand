# Integration test for copy on demand
# Tests:
#  * Full read/write
#  * Read/write blocks offset by one byte to test for off-by-one errors
#  * Read/write random sizes and offsets $fuzz_count times

block_size=4096 # 4k bytes
create_blocks=100 # How many $block_size blocks to generate for test
source_file='./test.dat'
backing_file='./test.dat.bak'
cod_executable='/usr/bin/copy-on-demand'
nbd_device='/dev/nbd0'
offset_test_offset=1
fuzz_count=100
cod_pid=0
init_sleep=1

function checkPrerequisites() {
    if [ ! -f "$cod_executable" ]
    then
        echo "Could not find copy on demand executable"
        exit 1
    fi

    if [ ! -b "$nbd_device" ]
    then
        echo "Could not find nbd block device, did you modprobe nbd?"
        exit 1
    fi

    if [ -f "$source_file" ] || [ -f "$backing_file" ]
    then
        echo "$source_file or $backing_file already exist, please delete or move them before running this test"
        exit 1
    fi
}

function generateSource() {
    rm $source_file 2> /dev/null
    # Generate a random source file
    echo "Generating $source_file"
    dd if=/dev/urandom bs=$block_size count=$create_blocks of=$source_file status=none
}

function bootCod() {
    $cod_executable -b $nbd_device $source_file $backing_file &
    cod_pid=$!
    sleep $init_sleep
}

function shutdownCod() {
    echo "Terminating cod $cod_pid"
    kill -SIGINT $cod_pid
    sleep 15
}

function endSuccessfulTest() {
    shutdownCod
    echo "Cleaning up backing file and intent log"
    rm $backing_file
    rm "$backing_file.bak"
}

function assertZero() {
    val=$1
    text=$2

    if [ $val -eq 0 ]
    then
        echo "$text PASSED"
    else
        echo "$text FAILED"
        echo "Terminating, leaving $source_file, $backing_file, and cod running in PID $cod_pid for debugging purposes"
        exit $val
    fi
}

function testFullRead() {
    bootCod

    cmp -s <(dd if=$nbd_device bs=$block_size count=$create_blocks status=none) <(dd if=$source_file bs=$block_size count=$create_blocks status=none)
    full_file_compare=$?

    assertZero $full_file_compare "Full file read comparision"

    endSuccessfulTest
}

function testFullWrite() {
    bootCod
    dd if=/dev/urandom bs=$block_size count=$create_blocks of=$nbd_device status=none
    sync
    cmp -s <(dd if=$nbd_device bs=$block_size count=$create_blocks status=none) <(dd if=$backing_file bs=$block_size count=$create_blocks status=none)
    full_file_compare=$?

    assertZero $full_file_compare "Full file write comparision"

    endSuccessfulTest
}

function testOffByOneOffsets() {
    bootCod

    current_block_offset=0
    cmp -s \
        <(dd if=$nbd_device skip=$(( ($current_block_offset * $block_size) + $block_size + $offset_test_offset)) bs=1 count=$block_size status=none) \
        <(dd if=$source_file skip=$(( ($current_block_offset * $block_size) + $block_size + $offset_test_offset)) bs=1 count=$block_size status=none)
    offset_block_compare=$?

    assertZero $offset_block_compare "Offset block read positive offset"

    current_block_offset=2

    cmp -s \
        <(dd if=$nbd_device skip=$(( ($current_block_offset * $block_size) + $block_size - $offset_test_offset)) bs=1 count=$block_size status=none) \
        <(dd if=$source_file skip=$(( ($current_block_offset * $block_size) + $block_size - $offset_test_offset)) bs=1 count=$block_size status=none)
    offset_block_compare=$?

    assertZero $offset_block_compare "Offset block read negative offset"

    current_block_offset=4
    dd if=/dev/urandom seek=$(( ($current_block_offset * $block_size) + $block_size + $offset_test_offset)) bs=1 count=$block_size of=$nbd_device status=none
    sync
    cmp -s \
        <(dd if=$nbd_device skip=$(( ($current_block_offset * $block_size) + $block_size + $offset_test_offset)) bs=1 count=$block_size status=none) \
        <(dd if=$backing_file skip=$(( ($current_block_offset * $block_size) + $block_size + $offset_test_offset)) bs=1 count=$block_size status=none)
    offset_block_compare=$?

    assertZero $offset_block_compare "Offset block write positive offset"

    current_block_offset=6

    dd if=/dev/urandom seek=$(( ($current_block_offset * $block_size) + $block_size - $offset_test_offset)) bs=1 count=$block_size of=$nbd_device status=none
    sync
    cmp -s \
        <(dd if=$nbd_device skip=$(( ($current_block_offset * $block_size) + $block_size - $offset_test_offset)) bs=1 count=$block_size status=none) \
        <(dd if=$backing_file skip=$(( ($current_block_offset * $block_size) + $block_size - $offset_test_offset)) bs=1 count=$block_size status=none)
    offset_block_compare=$?

    assertZero $offset_block_compare "Offset block write negative offset"

    endSuccessfulTest
}

function randomRead() {
    off=$((0 + RANDOM % ($block_size * $create_blocks - 1)))
    size=$((1 + RANDOM % ($block_size * 5)))

    cmp -s \
        <(dd if=$nbd_device skip=$off bs=1 count=$size status=none) \
        <(dd if=$source_file skip=$off bs=1 count=$size status=none)
    fuzz_read_status=$?

    assertZero $fuzz_read_status "Random read offset = $off, size = $size"
}

function randomWrite() {
    off=$((0 + RANDOM % ($block_size * $create_blocks - 1)))
    size=$((1 + RANDOM % ($block_size * 5)))

    dd if=/dev/urandom seek=$off bs=1 count=$size of=$nbd_device status=none
    sync
    cmp -s \
        <(dd if=$nbd_device skip=$off bs=1 count=$size status=none) \
        <(dd if=$backing_file skip=$off bs=1 count=$size status=none)
    fuzz_read_status=$?

    assertZero $fuzz_read_status "Random write offset = $off, size = $size"
}

# Do $fuzz_count random reads and writes
function fuzz() {
    bootCod
    for ((n=0;n<$fuzz_count;n++))
    do
        randomRead
    done
    endSuccessfulTest
    bootCod
    for ((n=0;n<$fuzz_count;n++))
    do
        randomWrite
    done
    endSuccessfulTest
}

checkPrerequisites
generateSource
testFullRead
testFullWrite
testOffByOneOffsets
fuzz

echo "All tests passed, cleaning up source file"
rm $source_file 2> /dev/null

exit 0
