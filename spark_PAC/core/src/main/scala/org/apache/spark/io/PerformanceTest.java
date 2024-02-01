package org.apache.spark.io;

import com.github.luben.zstd.ZstdOutputStream;
import com.ning.compress.lzf.LZFOutputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;
import net.jpountz.xxhash.StreamingXXHash32;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class PerformanceTest {
    protected  String compression_algorithm_name = "";
    protected  String inputFileName = "/home/zcp/paper_4_fgcs/sort_shuffle_raw_data/515865193042_temp_shuffle_3c2c2ae1-e36d-4080-9716-8c9b1fb957de_0";
    protected  String inputDir = "";
    protected  String outputFileName = "";
    protected  File[] fileList = {};
    protected  int read_interval = 0;
    protected  int speed_in_byte;
    protected int bufferSize = 32 * 1024;

    //exec_time in ms, data_size in MB
    public void cal_stream_speed(int data_size, int exec_time){
        if (data_size == 0 || exec_time == 0)
            this.speed_in_byte = 0;
        else
            this.speed_in_byte = (int) data_size*1024*1024/exec_time;
    }
    // interval in ms
    public void set_read_interval(int interval){
        this.read_interval = interval;
    }

    public File[] getFilesList() {
        File directoryPath = new File(this.inputDir);
        //List of all files and directories
        File filesList[] = directoryPath.listFiles();
        //System.out.println("List of files and directories in the specified directory:");
        return filesList;
        /*
        for (File file : filesList) {
            System.out.println("File name: " + file.getName());
            System.out.println("File path: " + file.getAbsolutePath());
            System.out.println("Size :" + file.getTotalSpace());
            System.out.println(" ");
        }
         */
    }
    public PerformanceTest(String inputDir,String compression_algorithm_name){
        // this.inputFileName = inputFileName;
        this.compression_algorithm_name = compression_algorithm_name;
        this.outputFileName = "/home/compression_performance_test-" + compression_algorithm_name;
        this.inputDir = inputDir;
        this.fileList = getFilesList();
    }

    public void compression_performance_test() throws IOException, InterruptedException {

        byte[] readBuffer = new byte[this.bufferSize];

        FileOutputStream s = new FileOutputStream(outputFileName);
        OutputStream out = null;
        if (compression_algorithm_name == "zstd") {
            int level = 1;
            int zstd_bufferSize = 32 * 1024;
            BufferedOutputStream zstd_out = new BufferedOutputStream(new ZstdOutputStream(s, level), zstd_bufferSize);
            out = zstd_out;
        }
        if (compression_algorithm_name == "kaezip2") {
            int kaezip_bufferSize = 32*1024;
            long stream_id = 0;
            KaezipOutputStream2 kaezip_out = new KaezipOutputStream2(s, kaezip_bufferSize, stream_id);
            out = kaezip_out;
        }
        if (compression_algorithm_name == "kaezip") {
            int kaezip_bufferSize = 512;
            KaezipOutputStream kaezip_out = new KaezipOutputStream(s, kaezip_bufferSize);
            out = kaezip_out;
        }
        if (compression_algorithm_name == "zlib") {
            int zlib_bufferSize = 32 * 1024;
            ZlibOutputStream zlib_out = new ZlibOutputStream(s,zlib_bufferSize);
            out = zlib_out;
        }

        if (compression_algorithm_name == "snappy") {
            int snappy_blocksize = 32*1024;
            SnappyOutputStream snappy_out = new SnappyOutputStream(s, snappy_blocksize);
            out = snappy_out;
        }
        if (compression_algorithm_name == "lzf") {
            LZFOutputStream lzf_out = new LZFOutputStream(s).setFinishBlockOnFlush(true);
            out = lzf_out;
        }
        if (compression_algorithm_name == "lz4") {
            int lz4_blocksize = 32*1024;
            LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
            XXHashFactory xxHashFactory  = XXHashFactory.fastestInstance();
            int defaultSeed  = 0x9747b28c; // LZ4BlockOutputStream.DEFAULT_SEED
            boolean syncFlush = false;
            LZ4BlockOutputStream lz4_out = new LZ4BlockOutputStream(
                    s,
                    lz4_blocksize,
                    lz4Factory.fastCompressor(),
                    xxHashFactory.newStreamingHash32(defaultSeed).asChecksum(),
                    syncFlush);
            out = lz4_out;
        }

        //File[] fileList = getFilesList();
        for(File file: fileList) {
            //System.out.println("filename," + file.getName());
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(file.getAbsolutePath()), bufferSize);

            while (in.available() > 0) {
                int byte_num = in.read(readBuffer);
                out.write(readBuffer);

                if (this.speed_in_byte != 0) {
                    this.read_interval = byte_num / this.speed_in_byte;
                    Thread.sleep(this.read_interval);
                }
            }
            in.close();
        }
        out.close();
    }

    public static void main(String args[]) throws IOException, InterruptedException {
        /*if (args.length < 1) {
            System.err.println("Usage: input compression_algorithm_name");
            System.exit(1);
        }

        String input = args[0];
         */

        String[] compression_algorithm_names = new String[] {"snappy","snappy","kaezip2", "kaezip", "zlib","lz4", "lzf","zstd"};
        String root_dir = "/home/zcp/paper_4_fgcs/compression_algorithm_analysis/sort/";
        String input_dir = root_dir + "sort_shuffle_raw_data";
        String dest_dir = root_dir + "/simulation/";
        String app_name = "sort";
        FileOutputStream performance_result = new FileOutputStream(dest_dir + "performance_result-"+app_name);

        for (String compression_algorithm_name : compression_algorithm_names) {
            String cmd = "/home/zcp/paper_4_fgcs/collectl.sh " + dest_dir + "sar-" + compression_algorithm_name;
            Process process = Runtime.getRuntime().exec(cmd);
            process.waitFor();
            long start_time = System.nanoTime();
            int input_data_size = 0;
            int exec_time = 5544;
            int repeat_count = 1;
            for(int i =0; i < repeat_count; i++) {
                PerformanceTest pt = new PerformanceTest(input_dir,compression_algorithm_name);
                pt.cal_stream_speed(input_data_size,exec_time);
                //pt.set_read_interval(8);
                pt.compression_performance_test();
            }
            long end_time = System.nanoTime();
            String result = "compression algorithm, compression time(ms), sleep_interval," + compression_algorithm_name
                    + "," + (end_time - start_time)/repeat_count/1000/1000 + "," + input_data_size*1024*1024/exec_time + "\n";
            performance_result.write(result.getBytes(StandardCharsets.UTF_8));
            System.out.println("compression algorithm, compression time(ms)," + compression_algorithm_name + "," + (end_time - start_time)/repeat_count/1000/1000);
            String cmd2 = "/home/zcp/paper_4_fgcs/kill_bash.sh sar";
            Process process2 = Runtime.getRuntime().exec(cmd2);
            process2.waitFor();

        }
        performance_result.close();
    }
}
