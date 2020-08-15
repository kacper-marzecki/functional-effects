package net.degoes.zio

import java.io.{ByteArrayInputStream, File, FileInputStream}

import zio._
import java.text.NumberFormat
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

import scala.io.Source

object Cat extends App {
  import zio.console._
  import zio.blocking._
  import java.io.IOException

  def autoClose[A <: AutoCloseable,B](closeable: A)(f: (A) ⇒ B): B = {
    try {
      f(closeable)
    } finally {
      closeable.close()
    }
  }
  /**
   * EXERCISE
   *
   * Using `effectBlockingIO`, implement a function to read a file on the
   * blocking thread pool, storing the result into a string.
   */
  def readFile(file: String): ZIO[Blocking, IOException, String] =
    effectBlockingIO(autoClose(scala.io.Source.fromFile(file))(_.mkString))

  /**
   * EXERCISE
   *
   * Implement a version of the command-line utility "cat", which dumps the
   * contents of the specified file to standard output.
   */
  def run(args: List[String]): ZIO[ZEnv, Nothing, ExitCode] =
    args match {
      case Nil => putStrLn("No file arg passed").exitCode
      case file ::  _ =>readFile(file).flatMap(it => putStrLn(it)).exitCode
    }
}

object CatBracket extends App {
  import zio.console._
  import zio.blocking._
  import java.io.IOException
  import scala.io.Source

  def open(file: String): ZIO[Blocking, IOException, Source] =
    effectBlockingIO(Source.fromFile(file))

  def close(source: Source): ZIO[Blocking, IOException, Unit] =
    effectBlockingIO(source.close())

  /**
   * EXERCISE
   *
   * Using `ZIO#bracket`, implement a safe version of `readFile` that cannot
   * fail to close the file, no matter what happens during reading.
   */
  def readFile(file: String): ZIO[Blocking, IOException, String] =
    ZIO.bracket(open(file))(s => UIO(s.close())) { s=>
      ZIO.succeed(s.mkString)
    }

  def run(args: List[String]): ZIO[ZEnv, Nothing, ExitCode] =
    (for {
      fileName <- ZIO
                   .fromOption(args.headOption)
                   .tapError(_ => putStrLn("You must specify a file name on the command line"))
      contents <- readFile(fileName)
      _        <- putStrLn(contents)
    } yield ()).exitCode
}

object SourceManaged extends App {
  import zio.console._
  import zio.blocking._
  import zio.duration._
  import java.io.IOException

  import scala.io.Source

  final class ZSource private (private val source: Source) {
    def execute[T](f: Source => T): ZIO[Blocking, IOException, T] =
      effectBlocking(f(source)).refineToOrDie[IOException]
  }
  object ZSource {

    /**
     * EXERCISE
     *
     * Use the `ZManaged.make` constructor to make a managed data type that
     * will automatically acquire and release the resource when it is used.
     */
    def make(file: String): ZManaged[Blocking, IOException, ZSource] = {
      // An effect that acquires the resource:
      val open = effectBlocking(new ZSource(Source.fromFile(file)))
        .refineToOrDie[IOException]

      // A function that, when given the resource, returns an effect that
      // releases the resource:
      val close: ZSource => ZIO[Blocking, Nothing, Unit] =
        _.execute(_.close()).orDie

      ZManaged.make(open)(close)
    }
  }

  /**
   * EXERCISE
   *
   * Using `ZManaged.foreachPar` and other functions as necessary, implement a function
   * to read the contents of all files in parallel, but ensuring that if anything
   * goes wrong during parallel reading, all files are safely closed.
   */
  def readFiles(
    files: List[String]
  ): ZIO[Blocking with Console, IOException, List[String]] =
    ZManaged.foreachPar(files)(s => ZSource.make(s))
      .use(files => ZIO.foreach(files)(_.execute(_.mkString)))
//    for {
//      files <-
//      content <-
//    } yield content




  /**
   * EXERCISE
   *
   * Implement a function that prints out all files specified on the
   * command-line. Only print out contents from these files if they
   * can all be opened simultaneously. Otherwise, don't print out
   * anything except an error message.
   */
  def run(args: List[String]): ZIO[ZEnv, Nothing, ExitCode] =
      readFiles(args)
        .map(_.map(putStrLn(_)))
        .flatMap(ZIO.collectAll(_))
      .exitCode
}

object CatIncremental extends App {
  import zio.console._
  import zio.blocking._
  import java.io.{ FileInputStream, IOException, InputStream }

  final case class FileHandle private (private val is: InputStream) {
    final def close: ZIO[Blocking, IOException, Unit] = effectBlockingIO(is.close())

    final def read: ZIO[Blocking, IOException, Option[Chunk[Byte]]] =
      effectBlockingIO {
        val array = Array.ofDim[Byte](1024)
        val len   = is.read(array)
        if (len < 0) None
        else Some(Chunk.fromArray(array).take(len))
      }
  }

  /**
   * EXERCISE
   *
   * Refactor `FileHandle` so that creating it returns a `ZManaged`, so that
   * it is impossible to forget to close an open handle.
   */
  object FileHandle {
    final def open(file: String): ZManaged[Blocking, IOException, FileHandle] =
      ZManaged.make(effectBlockingIO(new FileHandle(new FileInputStream(file))))(_.close.orDie)
  }


  val asString = (chunk: Chunk[Byte]) => chunk.map(_.toChar).mkString
  /**
   * EXERCISE
   *
   * Implement an incremental version of `cat` that pulls a chunk of bytes at
   * a time, stopping when there are no more chunks left.
   */
  def cat(fh: FileHandle): ZIO[Blocking with Console, IOException, Unit] =
    fh.read.flatMap {
      case Some(chunk) => putStr(asString(chunk)).flatMap(_ => cat(fh))
      case None => ZIO.succeed(())
    }

  /**
   * EXERCISE
   *
   * Implement an incremental version of the `cat` utility, using `ZIO#bracket`
   * or `ZManaged` to ensure the file is closed in the event of error or
   * interruption.
   */
  def run(args: List[String]): ZIO[ZEnv, Nothing, ExitCode] =
    args match {
      case file :: Nil =>
        /**
         * EXERCISE
         *
         * Open the specified file, safely create and use a file handle to
         * incrementally dump the contents of the file to standard output.
         */
        FileHandle.open(file).use(cat).exitCode

      case _ => putStrLn("Usage: cat <file>") as ExitCode(2)
    }
}
