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

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * @author <a href="mailto:haithanh0809@gmail.com">Nguyen Thanh Hai</a>
 * @version $Id$
 *
 */
public class FileSystemCat
{
	public static void main(String[] args) throws Exception
   {
		String uri = "hdfs://exoplatform:9000/user/haint/input-0/test.txt";
	   FileSystem fs = FileSystem.get(URI.create(uri), new Configuration());
	   InputStream in = null;
	   try {
	   	in = fs.open(new Path(uri));
	   	IOUtils.copyBytes(in, System.out, 4096, false);
	   } finally {
	   	IOUtils.closeStream(in);
	   }
	   System.out.println("---------------------------------------------------");
	   FSDataInputStream fsDataInputStream = null;
	   try {
	   	fsDataInputStream = fs.open(new Path(uri));
	   	IOUtils.copyBytes(fsDataInputStream, System.out, 256, false);
	   	System.out.println("---------------------------------------------------");
	   	fsDataInputStream.seek(0);
	   	IOUtils.copyBytes(fsDataInputStream, System.out, 256, false);
	   } finally {
	   	IOUtils.closeStream(fsDataInputStream);
	   }
   }
}
