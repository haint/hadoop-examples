/*
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package hadoop.examples.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

/**
 * @author <a href="mailto:haithanh0809@gmail.com">Nguyen Thanh Hai</a>
 * @version $Id$
 *
 */
public class SequenceFileWriteDemo
{
	
	private static String[] DATA = {
		"One, two hello world",
		"Three, four, goodbye baby",
		"Five, six, save money",
		"Seven, eight, write code more and more",
		"Nine, ten, rest in peace"
	};

	public static void main(String[] args) throws Exception
   {
	   Configuration conf = new Configuration();
	   String uri = "hdfs://exoplatform:9000/user/haint/temp.file";
	   Path path = new Path(uri);
	   FileSystem fs = FileSystem.get(URI.create(uri), conf);
	   
	   //
	   IntWritable key = new IntWritable();
	   Text value = new Text();
	   SequenceFile.Writer writer = null;
	   try {
	   	writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
	   	for(int i = 0; i < 100; i++) {
	   		key.set(100 - i);
	   		value.set(DATA[i % DATA.length]);
	   		System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
	   		writer.append(key, value);
	   	}
	   } finally {
	   	IOUtils.closeStream(writer);
	   }
   }
}
