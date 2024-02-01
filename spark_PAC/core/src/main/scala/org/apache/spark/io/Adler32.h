/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class Adler32 */

#ifndef _Included_Adler32
#define _Included_Adler32
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     java_util_zip_Adler32
 * Method:    update
 * Signature: (II)I
 */
JNIEXPORT jint JNICALL Java_Adler32_update
  (JNIEnv *, jclass, jint, jint);

/*
 * Class:     java_util_zip_Adler32
 * Method:    updateBytes
 * Signature: (I[BII)I
 */
JNIEXPORT jint JNICALL Java_Adler32_updateBytes
  (JNIEnv *, jclass, jint, jbyteArray, jint, jint);

/*
 * Class:     java_util_zip_Adler32
 * Method:    updateByteBuffer
 * Signature: (IJII)I
 */
JNIEXPORT jint JNICALL Java_Adler32_updateByteBuffer
  (JNIEnv *, jclass, jint, jlong, jint, jint);

#ifdef __cplusplus
}
#endif
#endif
