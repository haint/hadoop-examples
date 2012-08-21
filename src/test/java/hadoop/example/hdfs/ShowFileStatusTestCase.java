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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author <a href="mailto:haithanh0809@gmail.com">Nguyen Thanh Hai</a>
 * @version $Id$
 *
 */
public class ShowFileStatusTestCase
{
	private static MiniDFSCluster cluster; // use an in-process HDFS cluster for testing
	private static FileSystem fs;
	
	@BeforeClass
	public static void setUp() throws IOException {
		Configuration conf = new Configuration();
		if(System.getProperty("test.build.data") == null) {
			System.setProperty("test.build.data", "/tmp");
			cluster = new MiniDFSCluster(conf, 1, true, null);
			fs = cluster.getFileSystem();
			OutputStream out = fs.create(new Path("/dir/file"));
			out.write("content".getBytes("UTF-8"));
			out.close();
		}
	}
	
	@AfterClass
	public static void tearDown() throws IOException {
		if(fs != null) fs.close();
		if(cluster != null) cluster.shutdown();
	}
	
	@Test(expected = FileNotFoundException.class)
	public void throwsFileNotFoundForNonExistentFile() throws IOException {
		fs.getFileStatus(new Path("no-such-file"));
	}
	
	@Test
	public void fileStatusForFile() throws IOException {
		Path file = new Path("/dir/file");
		FileStatus stat = fs.getFileStatus(file);
		Assert.assertEquals("/dir/file", stat.getPath().toUri().getPath());
		Assert.assertFalse(stat.isDir());
		Assert.assertEquals(stat.getLen(), 7L);
		Assert.assertEquals(stat.getReplication(), 1);
		Assert.assertEquals(stat.getBlockSize(), 64 * 1024 * 1024L);
		Assert.assertEquals(stat.getOwner(), "haint");
		Assert.assertEquals(stat.getGroup(), "supergroup");
		Assert.assertEquals(stat.getPermission().toString(), "rw-r--r--");
	}
	
	@Test
	public void fileStatusForDirectory() throws IOException {
		Path dir = new Path("/dir");
		FileStatus stat = fs.getFileStatus(dir);
		Assert.assertTrue(stat.isDir());
		Assert.assertEquals(stat.getLen(), 0L);
		Assert.assertEquals(stat.getReplication(), 0L);
		Assert.assertEquals(stat.getBlockSize(), 0L);
		Assert.assertEquals(stat.getOwner(), "haint");
		Assert.assertEquals(stat.getGroup(), "supergroup");
		Assert.assertEquals(stat.getPermission().toString(), "rwxr-xr-x");
	}
}
