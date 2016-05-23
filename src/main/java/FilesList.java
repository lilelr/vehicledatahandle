import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

public class FilesList
{
//	hdfs://10.2.9.42:9000/ /lwlk_data/20140425_txt/ 320000.data
//	hdfs://10.2.9.42:9000/ /lwlk_data/ 320000.data
	public static Path[] getList(Configuration conf, String uriPath, String hdfsPath, String fileName) throws URISyntaxException, IOException
 	{
	URI uri = new URI(uriPath); 
	FileSystem fs = FileSystem.get(uri, conf);


	FileStatus[] status = fs.listStatus(new Path(uriPath + hdfsPath.substring(1)));
	ArrayList<Path> r = new ArrayList<Path>();
	for (FileStatus fileItem : status)
	{
//		String path = file.getPath().toString();
//		if (!path.endsWith("/")) path += "/";
//		path += fileName;
//		Path p = new Path(path);
//		if (fs.exists(p))
//		r.add(p);

		if (fileItem.isDirectory()){
			if (fileItem.getPath().toString().contains("_txt")){
				// 是数据文件/lwlk_data/20140425_txt
				System.out.println(fileItem.getPath().toString());
				FileStatus[] dataStatus = fs.listStatus(fileItem.getPath());
				for(FileStatus dataFileItem : dataStatus){
					String path = dataFileItem.getPath().toString();
					if (!path.endsWith("/")) path += "/";
					path += fileName;
					Path p = new Path(path);
					if (fs.exists(p)){
						System.out.println(p);
						r.add(p);
					}

				}
			}
		}
	}
	return r.toArray(new Path[r.size()]);
//	System.out.println(file.getPath().toString());
	}
}