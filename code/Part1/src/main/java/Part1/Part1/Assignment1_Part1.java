package Part1.Part1;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

public class Assignment1_Part1 {
	private static Configuration config = new Configuration();
	public static void main(String args[]) {
        String[] urls = {
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2",
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2",
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2",
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2",
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2",
                "http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2"
        };
        String dest_location = args[0];
        config.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
        config.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
        String[] output_Files = {
                "part1_1.txt.bz2",
                "part1_2.txt.bz2",
                "part1_3.txt.bz2",
                "part1_4.txt.bz2",
                "part1_5.txt.bz2",
                "part1_6.txt.bz2"
        };
        int count = 0;
        try {
            for (String url: urls) {
                String download = dest_location + File.separator + output_Files[count];
                boolean status = downloadBooks(url, download);
                if (status) {
                    System.out.println("*****Downloaded file:"+output_Files[count]+"*****");
                    System.out.println("*****Decompressing file:" + output_Files[count]+"*****");
                    decompress(download);
                    System.out.println("*****Deleting compressed file: " + output_Files[count]+"*****");
                    deleteFiles(download);                    
                }
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	//Download books
    private static boolean downloadBooks(String source, String dest) throws IOException{
        URL url = new URL(source);
        InputStream inStream = new BufferedInputStream(url.openStream());
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(dest), conf);
        if (!fileSystem.exists(new Path(dest))) {
            OutputStream outStream = fileSystem.create(new Path(dest), new Progressable() {
                public void progress() {
                    System.out.print(".");
                }
            });
            IOUtils.copyBytes(inStream, outStream, 4096, true);
            return true;
        } else {
            System.out.println("File with this name already exists");
            return false;
        }
    }
    //Decompress the files
    private static void decompress(String file) throws IOException{
        FileSystem fs = FileSystem.get(URI.create(file), config);
        Path input = new Path(file);
        CompressionCodecFactory factory = new CompressionCodecFactory(config);
        CompressionCodec codec = factory.getCodec(input);
        if (codec == null) {
            System.err.println("No codec found for" + file);
            System.exit(1);
        }
        String finalUri = CompressionCodecFactory.removeSuffix(file, codec.getDefaultExtension());
        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            inputStream = codec.createInputStream(fs.open(input));
            outputStream = fs.create(new Path(finalUri));
            IOUtils.copyBytes(inputStream, outputStream, config);
        } finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(outputStream);
        }
    }
    //Delete files in bz2 format
    private static void deleteFiles(String file) throws IOException{
        FileSystem fs = FileSystem.get(URI.create(file), config);
        if (fs.exists(new Path(file))) {
            fs.delete(new Path(file), true);
        }
    }    
}
