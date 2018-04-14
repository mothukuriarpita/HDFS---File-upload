package JavaHDFS.Part2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import java.io.*;
import java.io.BufferedInputStream;
import java.io.File;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.net.*;
public class Part2 {
	
	public static void main(String[] args) {
		String dest=args[0];
		String url="https://corpus.byu.edu/nowtext-samples/text.zip";
		String dst=dest+"/";
		String dest_location=dest+"/text";
		boolean status;
		try {
			status = download(url,dest_location);
			if (status)
			{
				InputStream input = null;
			    OutputStream output = null;
				String u = dest_location;
				Configuration config = new Configuration();
				config.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
				config.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
				FileSystem fs = FileSystem.get(URI.create(u), config);					    
				Path inputPath = new Path(u);
				try {
					     ZipInputStream inputzip = new ZipInputStream((fs.open(inputPath)));
					     ZipEntry zipEntry;
				            while ((zipEntry = inputzip.getNextEntry()) != null) {
					            String filePath = dst + File.separator + zipEntry.getName();
					            if (!zipEntry.isDirectory()) 
					            {
					            	output = fs.create(new Path(dst+"/"+zipEntry.getName()));
					            	byte[] in = new byte[4096];
					                int read = 0;
					            	while (( read = inputzip.read(in)) != -1)
					            	{
					                    output.write(in, 0, read);
					                }
					            }
					            else 
					            {
					                File dir = new File(filePath);
					                dir.mkdir();
					            }
					            inputzip.closeEntry();
					            zipEntry = inputzip.getNextEntry();
					        }
					        fs.delete(inputPath);
						    inputzip.close();  
					        output.close();
					      
					      } 
					    finally
					    {
					      IOUtils.closeStream(input);
					      IOUtils.closeStream(output);
					    }				
			}
			else
			{
				System.out.println("File decompression not possible");
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		}
	public static boolean download(String furl, String dir)
            throws IOException {
        	URL url1 = new URL(furl);
       	    URLConnection conc = url1.openConnection();          
            conc.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11");
            conc.connect();
            InputStream inStream = new BufferedInputStream(conc.getInputStream());
            Configuration conf = new Configuration();
            conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
            conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
            FileSystem fs = FileSystem.get(URI.create(dir), conf);
            if(!fs.exists(new Path(dir))){
            	OutputStream outStream = fs.create(new Path(dir));
                        	IOUtils.copyBytes(inStream, outStream, 4096, true);
            	return true;
            }
            else{
            	System.out.println("File with this name already exists");
            	return false;
            }
    }	

   
}
