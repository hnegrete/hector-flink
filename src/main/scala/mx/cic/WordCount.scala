package mx.cic
// Librerías con programas para implementar transformaciones en data streams
import java.io.File
// Librería para manipular archivos
import org.apache.flink.api.scala._
// Librería para convertir una cadena de caracteres en una ruta
import java.nio.file.Paths
// Librería para exportar representaciones formateadas de objetos a archivos de texto
import java.io.PrintWriter

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 */

object WordCount {
  // Función para obtener los archivos contenidos en un directorio
  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  // Función para convertir una ruta relativa a la ruta absoluta del cliente
  def toAbsolutePath(maybeRelative: String): String = {
    val path = Paths.get(maybeRelative)
    var effectivePath = path
    if (!path.isAbsolute) {
      val base = Paths.get("")
      effectivePath = base.resolve(path).toAbsolutePath
    }
    effectivePath.normalize.toString
  }

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    // Obtener lista de archivos contenidos en la carpeta de entrada
    val myList = getListOfFiles(toAbsolutePath(args(0)))
    // Cadena de caracteres para concatenar los archivos
    var listaArchivos = ""
    // Cadena de caracteres para concatenar los totales de palabras contadas en cada archivo
    var listaTotales = ""
    // Entero para acumular el total de palabras contadas en todos los archivos
    var valorTotal = 0

    // Realizar el conteo de palabras por archivo
    for(i<-0 to myList.size-1){
      // Valor con la información contenida en el archivo i
      val text = env.readTextFile(myList(i).toString)
      // Contar el total de las palabras contenidas en el archivo i
      val totales = text.flatMap { _.toLowerCase.split("\\W+") }.map { (_, 1) }.sum(1)
      // Extraer el valor del total de palabras
      val x = totales.collect()
      val (llave,valor) = x(0)
      // Acumular los nombres de los archivos
      listaArchivos = listaArchivos + "File" + (i+1) + ", "
      // Acumular los valores de las palabras contadas en cada archivo
      listaTotales = listaTotales + valor + ", "
      // Acumular el total de palabras contenidas en los archivos
      valorTotal = valorTotal + valor
    }

    // Exportar resultados
    var resultados = listaArchivos.slice(0,listaArchivos.length-2) + "\n" + "****************************************\n"
    resultados = resultados + listaTotales.slice(0,listaTotales.length-2) + "\n" + "****************************************\n"
    resultados = resultados + valorTotal + "\n" + "****************************************\n"

    for(i<-0 to myList.size-1){
      val text = env.readTextFile(myList(i).toString)
      val counts = text.flatMap { _.toLowerCase.split("\\W+") }.map { (_, 1) }.groupBy(0).sum(1)
      val x = counts.collect()
      val f = new File(myList(i).toString)
      resultados = resultados + f.getName().toString + "\n"
      resultados = resultados + x.toString().slice(7,x.toString().size-1)
      resultados = resultados.replace("), (",")\n(") + "\n"
      resultados = resultados.replace("(","").replace(")","")
    }
    new PrintWriter(toAbsolutePath(args(1))) { write(resultados); close }
  }
}
