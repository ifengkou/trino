/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.udf.function.roaringbitmap;

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

/**
 * @author shenlongguang https://github.com/ifengkou
 * @date: 2021/12/8
 */
public class RoaringBitMapUtils
{
    private RoaringBitMapUtils()
    {
    }

    /**
     * 序列化一个Roaring64NavigableMap
     *
     * @param rbm
     * @return
     */
    public static byte[] serialize64(Roaring64NavigableMap rbm)
    {
        ByteBuffer byteBuffer = null;
        //ByteBufferOut byteBufferOut = null;
        ByteArrayOutputStream baos = null;
        DataOutputStream dataOutputStream = null;
        byte[] bytes = null;
        try {
            if (rbm == null) {
                rbm = new Roaring64NavigableMap();
            }
            rbm.runOptimize();
            //ByteBuffer buffer = ByteBuffer.allocate((int)rbm.serializedSizeInBytes());
            //byteBuffer = ByteBuffer.allocate(Long.BYTES + (int) rbm.serializedSizeInBytes());
            baos = new ByteArrayOutputStream((int) rbm.serializedSizeInBytes());
            //byteBufferOut = new ByteBufferBackedOutputStream(byteBuffer);
            dataOutputStream = new DataOutputStream(baos);
            rbm.serialize(dataOutputStream);
            bytes = byteBuffer.array();
        }
        catch (Exception e) {
            throw new TrinoException(StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR, "rbm serialize error: %s" + e.getMessage());
        }
        finally {
            if (byteBuffer != null) {
                try {
                    byteBuffer.clear();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (baos != null) {
                try {
                    baos.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (dataOutputStream != null) {
                try {
                    dataOutputStream.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        return bytes;
    }

    /**
     * 反序列化一个Roaring64NavigableMap
     *
     * @param bytes
     * @return
     */
    public static Roaring64NavigableMap deserialize64(byte[] bytes)
    {
        ByteArrayInputStream byteArrayInputStream = null;
        DataInputStream dis = null;
        Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
        try {
            byteArrayInputStream = new ByteArrayInputStream(bytes);
            dis = new DataInputStream(byteArrayInputStream);
            bitmap.deserialize(dis);
        }
        catch (Exception e) {
            throw new TrinoException(StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR, "rbm deserialize error: %s" + e.getMessage());
        }
        finally {
            if (byteArrayInputStream != null) {
                try {
                    byteArrayInputStream.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (dis != null) {
                try {
                    dis.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        return bitmap;
    }

    public static RoaringBitmap deserialize32(byte[] bytes)
    {
        ByteArrayInputStream byteArrayInputStream = null;
        DataInputStream dis = null;
        RoaringBitmap bitmap = new RoaringBitmap();
        try {
            byteArrayInputStream = new ByteArrayInputStream(bytes);
            dis = new DataInputStream(byteArrayInputStream);
            bitmap.deserialize(dis);
        }
        catch (Exception e) {
            throw new TrinoException(StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR, "rbm deserialize error: %s" + e.getMessage());
        }
        finally {
            if (byteArrayInputStream != null) {
                try {
                    byteArrayInputStream.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (dis != null) {
                try {
                    dis.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        return bitmap;
    }

    /**
     * 合并两个rbm
     *
     * @param curRbm
     * @param othRbm
     * @return
     */
    public static Roaring64NavigableMap mergeRbm(Roaring64NavigableMap curRbm, Roaring64NavigableMap othRbm)
    {
        Roaring64NavigableMap result;
        if (curRbm == null && othRbm == null) {
            result = new Roaring64NavigableMap();
        }
        else if (curRbm == null) {
            result = othRbm;
        }
        else if (othRbm == null) {
            result = curRbm;
        }
        else {
            curRbm.or(othRbm);
            result = curRbm;
        }

        return result;
    }
}
