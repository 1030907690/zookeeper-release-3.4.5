package com.zzq.test.nio;

import sun.security.krb5.internal.NetClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * @author Zhou Zhong Qing
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: Nio Client
 * @date 2019/6/14 9:52
 */
public class NioClient {

    public static void main(String[] args) throws Exception {
     /*   Socket socket = new Socket("localhost", 9999);
        InputStream is = socket.getInputStream();
        OutputStream os = socket.getOutputStream();

        // 先向服务端发送数据
        os.write("Hello, Server!\0".getBytes());

        // 读取服务端发来的数据
        int b;
        while ((b = is.read()) != 0) {
            System.out.print((char) b);
        }
        System.out.println();

        socket.close();*/


        try {
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.connect(new InetSocketAddress("127.0.0.1", 9999));

            socketChannel.configureBlocking(true);

            ByteBuffer writeBuffer = ByteBuffer.allocate(1024);
            writeBuffer.put("hello world!".getBytes());
            writeBuffer.flip();

            socketChannel.write(writeBuffer);


            socketChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}