/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om.callqueue;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.thirdparty.protobuf.CodedInputStream;
import org.apache.hadoop.thirdparty.protobuf.CodedOutputStream;
import org.apache.hadoop.thirdparty.protobuf.Message;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class OzoneRpcWritable implements Writable {

  static OzoneRpcWritable wrap(Object o) {
    if (o instanceof OzoneRpcWritable) {
      return (OzoneRpcWritable)o;
    } else if (o instanceof Message) {
      return new OzoneRpcWritable.ProtobufWrapper((Message)o);
    } else if (o instanceof com.google.protobuf.Message) {
      return new OzoneRpcWritable.ProtobufWrapperLegacy((com.google.protobuf.Message) o);
    } else if (o instanceof Writable) {
      return new OzoneRpcWritable.WritableWrapper((Writable)o);
    }
    throw new IllegalArgumentException("Cannot wrap " + o.getClass());
  }

  // don't support old inefficient Writable methods.
  @Override
  public final void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException();
  }
  @Override
  public final void write(DataOutput out) throws IOException {
    throw new UnsupportedOperationException();
  }

  // methods optimized for reduced intermediate byte[] allocations.
  abstract void writeTo(OzoneResponseBuffer out) throws IOException;
  abstract <T> T readFrom(ByteBuffer bb) throws IOException;

  // adapter for Writables.
  static class WritableWrapper extends OzoneRpcWritable {
    private final Writable writable;

    WritableWrapper(Writable writable) {
      this.writable = writable;
    }

    @Override
    public void writeTo(OzoneResponseBuffer out) throws IOException {
      writable.write(out);
    }

    @SuppressWarnings("unchecked")
    @Override
    <T> T readFrom(ByteBuffer bb) throws IOException {
      // create a stream that may consume up to the entire ByteBuffer.
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(
          bb.array(), bb.position() + bb.arrayOffset(), bb.remaining()));
      try {
        writable.readFields(in);
      } finally {
        // advance over the bytes read.
        bb.position(bb.limit() - in.available());
      }
      return (T)writable;
    }
  }

  // adapter for Protobufs.
  static class ProtobufWrapper extends OzoneRpcWritable {
    private Message message;

    ProtobufWrapper(Message message) {
      this.message = message;
    }

    Message getMessage() {
      return message;
    }

    @Override
    void writeTo(OzoneResponseBuffer out) throws IOException {
      int length = message.getSerializedSize();
      length += CodedOutputStream.computeUInt32SizeNoTag(length);
      out.ensureCapacity(length);
      message.writeDelimitedTo(out);
    }

    @SuppressWarnings("unchecked")
    @Override
    <T> T readFrom(ByteBuffer bb) throws IOException {
      // using the parser with a byte[]-backed coded input stream is the
      // most efficient way to deserialize a protobuf.  it has a direct
      // path to the PB ctor that doesn't create multi-layered streams
      // that internally buffer.
      CodedInputStream cis = CodedInputStream.newInstance(
          bb.array(), bb.position() + bb.arrayOffset(), bb.remaining());
      try {
        cis.pushLimit(cis.readRawVarint32());
        message = message.getParserForType().parseFrom(cis);
        cis.checkLastTagWas(0);
      } finally {
        // advance over the bytes read.
        bb.position(bb.position() + cis.getTotalBytesRead());
      }
      return (T)message;
    }
  }

  // adapter for Protobufs.
  static class ProtobufWrapperLegacy extends OzoneRpcWritable {
    private com.google.protobuf.Message message;

    ProtobufWrapperLegacy(com.google.protobuf.Message message) {
      this.message = message;
    }

    com.google.protobuf.Message getMessage() {
      return message;
    }

    @Override
    void writeTo(OzoneResponseBuffer out) throws IOException {
      int length = message.getSerializedSize();
      length += com.google.protobuf.CodedOutputStream.
          computeUInt32SizeNoTag(length);
      out.ensureCapacity(length);
      message.writeDelimitedTo(out);
    }

    @SuppressWarnings("unchecked")
    @Override
    <T> T readFrom(ByteBuffer bb) throws IOException {
      // using the parser with a byte[]-backed coded input stream is the
      // most efficient way to deserialize a protobuf.  it has a direct
      // path to the PB ctor that doesn't create multi-layered streams
      // that internally buffer.
      com.google.protobuf.CodedInputStream cis =
          com.google.protobuf.CodedInputStream.newInstance(
              bb.array(), bb.position() + bb.arrayOffset(), bb.remaining());
      try {
        cis.pushLimit(cis.readRawVarint32());
        message = message.getParserForType().parseFrom(cis);
        cis.checkLastTagWas(0);
      } finally {
        // advance over the bytes read.
        bb.position(bb.position() + cis.getTotalBytesRead());
      }
      return (T)message;
    }
  }

  /**
   * adapter to allow decoding of writables and protobufs from a byte buffer.
   */
  public static class Buffer extends OzoneRpcWritable {
    private ByteBuffer bb;

    public static Buffer wrap(ByteBuffer bb) {
      return new Buffer(bb);
    }

    Buffer() {}

    Buffer(ByteBuffer bb) {
      this.bb = bb;
    }

    ByteBuffer getByteBuffer() {
      return bb;
    }

    @Override
    void writeTo(OzoneResponseBuffer out) throws IOException {
      out.ensureCapacity(bb.remaining());
      out.write(bb.array(), bb.position() + bb.arrayOffset(), bb.remaining());
    }

    @SuppressWarnings("unchecked")
    @Override
    <T> T readFrom(ByteBuffer bb) throws IOException {
      // effectively consume the rest of the buffer from the callers
      // perspective.
      this.bb = bb.slice();
      bb.limit(bb.position());
      return (T)this;
    }

    public <T> T newInstance(Class<T> valueClass,
                             Configuration conf) throws IOException {
      T instance;
      try {
        // this is much faster than ReflectionUtils!
        instance = valueClass.newInstance();
        if (instance instanceof Configurable) {
          ((Configurable)instance).setConf(conf);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return getValue(instance);
    }

    public <T> T getValue(T value) throws IOException {
      return OzoneRpcWritable.wrap(value).readFrom(bb);
    }

    public int remaining() {
      return bb.remaining();
    }
  }
}
