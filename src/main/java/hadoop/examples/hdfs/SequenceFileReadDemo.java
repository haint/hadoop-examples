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
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * @author <a href="mailto:haithanh0809@gmail.com">Nguyen Thanh Hai</a>
 * @version $Id$
 *
 */
public class SequenceFileReadDemo
{

	public static void main(String[] args) throws Exception
   {
		Configuration conf = new Configuration();
	   String uri = "hdfs://exoplatform:9000/user/haint/temp.file";
	   Path path = new Path(uri);
	   FileSystem fs = FileSystem.get(URI.create(uri), conf);
	   
	   SequenceFile.Reader reader = null;
	   try {
	   	reader = new SequenceFile.Reader(fs, path, conf);
	   	Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
	   	Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
	   	long position = reader.getPosition();
	   	
	   	//
	   	while(reader.next(key, value)) {
	   		String syncSee = reader.syncSeen() ? "*" : "";
	   		System.out.printf("[%s%s]\t%s\t%s\n", position, syncSee, key, value);
	   		position = reader.getPosition();
	   	}
	   } finally {
	   	IOUtils.closeStream(reader);
	   }
   }
}
