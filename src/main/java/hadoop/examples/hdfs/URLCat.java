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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * @author <a href="mailto:haithanh0809@gmail.com">Nguyen Thanh Hai</a>
 * @version $Id$
 *
 */
public class URLCat
{

	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	public static void main(String[] args)
   {
	   InputStream is = null;
	   try {
	   	is = new URL("hdfs://192.168.56.1:9000/user/haint/input-0/test.txt").openStream();
	   	BufferedInputStream bis = new BufferedInputStream(is);
	   	byte[] buff = new byte[256];
	   	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	   	for(int length = bis.read(buff); length != -1; length = bis.read(buff)) {
	   		baos.write(buff, 0, length);
	   	}
	   	System.out.println(new String(baos.toByteArray()));
	   } catch(IOException e) {
	   	IOUtils.closeStream(is);
	   }
   }
}
