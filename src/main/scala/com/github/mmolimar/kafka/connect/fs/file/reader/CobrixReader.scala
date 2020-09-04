package com.github.mmolimar.kafka.connect.fs.file.reader

import com.github.mmolimar.kafka.connect.fs.file.reader.CobolFileReader.StructHandler
import za.co.absa.cobrix.cobol.reader.parameters.ReaderParameters
import za.co.absa.cobrix.cobol.reader.{VarLenNestedReader, VarLenReader}

import scala.collection.Seq

protected object CobrixReader {

  def varLenReader(copybookContent: String, params: ReaderParameters): VarLenReader = {
    new VarLenNestedReader[java.util.Map[String, AnyRef]](Seq(copybookContent), params, new StructHandler())
  }

}
