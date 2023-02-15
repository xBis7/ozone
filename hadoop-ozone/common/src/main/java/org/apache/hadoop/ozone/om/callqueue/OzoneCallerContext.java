package org.apache.hadoop.ozone.om.callqueue;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ipc.CallerContext;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class OzoneCallerContext {
  public static final Charset SIGNATURE_ENCODING = StandardCharsets.UTF_8;
  /** The caller context.
   *
   * It will be truncated if it exceeds the maximum allowed length in
   * server. The default length limit is
   * {@link org.apache.hadoop.fs.CommonConfigurationKeysPublic#HADOOP_CALLER_CONTEXT_MAX_SIZE_DEFAULT}
   */
  private final String context;

  /** The caller's signature for validation.
   *
   * The signature is optional. The null or empty signature will be abandoned.
   * If the signature exceeds the maximum allowed length in server, the caller
   * context will be abandoned. The default length limit is
   * {@link org.apache.hadoop.fs.CommonConfigurationKeysPublic#HADOOP_CALLER_CONTEXT_SIGNATURE_MAX_SIZE_DEFAULT}
   */
  private final byte[] signature;

  private OzoneCallerContext(Builder builder) {
    this.context = builder.context;
    this.signature = builder.signature;
  }

  public String getContext() {
    return context;
  }

  public byte[] getSignature() {
    return signature == null ?
        null : Arrays.copyOf(signature, signature.length);
  }

  @InterfaceAudience.Private
  public boolean isContextValid() {
    return context != null && !context.isEmpty();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder().append(context).toHashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    } else if (obj == this) {
      return true;
    } else if (obj.getClass() != getClass()) {
      return false;
    } else {
      OzoneCallerContext rhs = (OzoneCallerContext) obj;
      return new EqualsBuilder()
          .append(context, rhs.context)
          .append(signature, rhs.signature)
          .isEquals();
    }
  }

  @Override
  public String toString() {
    if (!isContextValid()) {
      return "";
    }
    String str = context;
    if (signature != null) {
      str += ":";
      str += new String(signature, SIGNATURE_ENCODING);
    }
    return str;
  }

  /** The caller context builder. */
  public static final class Builder {
    private final String context;
    private byte[] signature;

    public Builder(String context) {
      this.context = context;
    }

    public Builder setSignature(byte[] signature) {
      if (signature != null && signature.length > 0) {
        this.signature = Arrays.copyOf(signature, signature.length);
      }
      return this;
    }

    public OzoneCallerContext build() {
      return new OzoneCallerContext(this);
    }
  }

  /**
   * The thread local current caller context.
   * <p>
   * Internal class for deferred singleton idiom.
   */
  private static final class CurrentCallerContextHolder {
    static final ThreadLocal<OzoneCallerContext> CALLER_CONTEXT =
        new InheritableThreadLocal<>();
  }

  public static OzoneCallerContext getCurrent() {
    return CurrentCallerContextHolder.CALLER_CONTEXT.get();
  }

  public static void setCurrent(OzoneCallerContext callerContext) {
    CurrentCallerContextHolder.CALLER_CONTEXT.set(callerContext);
  }
}
