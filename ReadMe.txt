----------------------------------------------------------
External_Sort_Java
----------------------------------------------------------
Open C3.Large instance

./gensort -a 100000000 /mnt/raid/input
mkdir -p /mnt/raid/tmp
javac sortFile_multithreaded.java
java sortFile_multithreaded 1 >> outputlog.txt (for 1 Thread)
java sortFile_multithreaded 2 >> outputlog.txt (for 2 Thread)
java sortFile_multithreaded 4 >> outputlog.txt (for 4 Thread)
java sortFile_multithreaded 8 >> outputlog.txt (for 8 Thread)

unix2dos /mnt/raid/output
./valsort /mnt/raid/output
----------------------------------------------------------