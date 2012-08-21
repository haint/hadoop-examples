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
package hadoop.example.hdfs;

import java.io.IOException;
import java.net.URI;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * @author <a href="mailto:haithanh0809@gmail.com">Nguyen Thanh Hai</a>
 * @version $Id$
 *
 */
public class CoherencyTestCase extends Assert
{

	@Test
	public void test() throws IOException {
		String uri = "hdfs://exoplatform:9000/user/haint/temp.file";
		Path path = new Path(uri);
		FileSystem fs = FileSystem.get(URI.create(uri), new Configuration());
		try {
			FSDataOutputStream output = fs.create(path);
			assertTrue(fs.exists(path));
			
			//
			output.write("content".getBytes());
			output.flush();
			assertEquals(fs.getFileStatus(path).getLen(), 0L);
			
			output.sync();
			assertEquals(fs.getFileStatus(path).getLen(), 0L);
			
			output.close();
			assertEquals(fs.getFileStatus(path).getLen(), "content".length());
		} finally {
			fs.delete(path, false);
		}
	}
}
