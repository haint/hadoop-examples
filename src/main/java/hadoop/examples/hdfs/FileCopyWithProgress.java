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

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

/**
 * @author <a href="mailto:haithanh0809@gmail.com">Nguyen Thanh Hai</a>
 * @version $Id$
 *
 */
public class FileCopyWithProgress
{
	public static void main(String[] args) throws Exception
   {
	   String uri = "hdfs://exoplatform:9000/user/haint/input-1/hadoop-0.20.203.1-SNAPSHOT.tar.gz";
	   InputStream in = new BufferedInputStream(new FileInputStream("/home/haint/java/research/hadoop-0.20.203.1-SNAPSHOT.tar.gz"));
	   FileSystem fs = FileSystem.get(URI.create(uri), new Configuration());
	   OutputStream out = fs.create(new Path(uri), new Progressable()
		{
			public void progress()
			{
				System.out.print(".");
			}
		});
	   
	   IOUtils.copyBytes(in, out, 4096, true);
   }
}
