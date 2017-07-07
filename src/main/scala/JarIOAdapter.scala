package utils

import java.io.{IOException, InputStream, OutputStream}

import com.hankcs.hanlp.corpus.io.IIOAdapter

/**
  * Created by linjiang on 2017/5/18.
  */
class JarIOAdapter extends IIOAdapter{
  override def create(path: String): OutputStream = new OutputStream() {
    @throws[IOException]
    override def write(b: Int): Unit = {   }
  }

  override def open(path: String): InputStream = classOf[JarIOAdapter].getClassLoader.getResourceAsStream(path)
}
