
INCDIR += -I /home/zcp/Downloads/jdk1.8_source_code/build/linux-x86_64-normal-server-release/jdk/include/
INCDIR += -I /home/zcp/Downloads/jdk1.8_source_code/build/linux-x86_64-normal-server-release/jdk/include/linux/
INCDIR += -I /home/zcp/Downloads/jdk1.8_source_code/jdk/src/share/native/common/
INCDIR += -I /home/zcp/Downloads/jdk1.8_source_code/jdk/src/solaris/native/common/
INCDIR += -I  /home/zcp/Downloads/jdk1.8_source_code/build/linux-x86_64-normal-server-release/jdk/gensrc_headers/
INCDIR += -I  /home/zcp/Downloads/jdk1.8_source_code/jdk/src/share/javavm/export/
INCDIR += -I  /home/zcp/Downloads/jdk1.8_source_code/jdk/src/solaris/javavm/export/
INCDIR += -I  /home/zcp/Downloads/jdk1.8_source_code/jdk/src/share/native/java/io/
INCDIR += -I  /home/zcp/Downloads/jdk1.8_source_code/jdk/src/solaris/native/java/io/

# Include Libs.

LIBDIR := -L/usr/local/kaezip/lib

# The flags
CFLAGS    := -g -Wall -lz -fPIC -shared
LDFLAGS   := $(LIBDIR)

all: Adler32
objects = Adler32.c CRC32.c Deflater.c Inflater.c ZipFile.c zip_util.c 
Adler32 : $(objects)
	gcc  $(CFLAGS) $(LDFLAGS) $(INCDIR)  $(objects) -o libkaezip.so

.PHONY : clean
clean :
	-rm -f libkaezip.so
