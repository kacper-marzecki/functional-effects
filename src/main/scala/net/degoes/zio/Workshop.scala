package net.degoes.zio

import zio._

object HelloWorld extends App {
  import zio.console._

  /**
    * EXERCISE 1
    *
    * Implement a simple "Hello World" program using the effect returned by `putStrLn`.
    */
  def run(args: List[String]): ZIO[ZEnv, Nothing, Int] =
    ZIO.succeed(0)
}

object ErrorConversion extends App {
  val StdInputFailed = 1

  import zio.console._

  val failed =
    putStrLn("About to fail...") *>
      ZIO.fail("Uh oh!") *>
      putStrLn("This will NEVER be printed!")

  /**
    * EXERCISE 2
    *
    * Using `ZIO#orElse` or `ZIO#fold`, have the `run` function compose the
    * preceding `failed` effect into the effect that `run` returns.
    */
  def run(args: List[String]): ZIO[ZEnv, Nothing, Int] = ???
}

object PromptName extends App {
  val StdInputFailed = 1

  import zio.console._

  /**
    * EXERCISE 3
    *
    * Implement a simple program that asks the user for their name (using
    * `getStrLn`), and then prints it out to the user (using `putStrLn`).
    */
  def run(args: List[String]): ZIO[ZEnv, Nothing, Int] = ???
}

object ZIOTypes {
  type ??? = Nothing

  /**
    * EXERCISE 4
    *
    * Provide definitions for the ZIO type aliases below.
    */
  type Task[+A] = ???
  type UIO[+A] = ???
  type RIO[-R, +A] = ???
  type IO[+E, +A] = ???
  type URIO[-R, +A] = ???
}

object NumberGuesser extends App {
  import zio.console._
  import zio.random._

  def analyzeAnswer(random: Int, guess: String) =
    if (random.toString == guess.trim) putStrLn("You guessed correctly!")
    else putStrLn("You did not guess correctly. The answer was ${random}")

  /**
    * EXERCISE 5
    *
    * Choose a random number (using `nextInt`), and then ask the user to guess
    * the number, feeding their response to `analyzeAnswer`, above.
    */
  def run(args: List[String]): ZIO[ZEnv, Nothing, Int] =
    ???
}

object AlarmApp extends App {
  import zio.console._
  import zio.duration._
  import java.io.IOException

  /**
    * EXERCISE 6
    *
    * Create an effect that will get a `Duration` from the user, by prompting
    * the user to enter a decimal number of seconds.
    */
  lazy val getAlarmDuration: ZIO[Console, IOException, Duration] = {
    def parseDuration(input: String): IO[NumberFormatException, Duration] =
      ???

    def fallback(input: String): ZIO[Console, IOException, Duration] =
      ???

    ???
  }

  /**
    * EXERCISE 7
    *
    * Create a program that asks the user for a number of seconds to sleep,
    * sleeps the specified number of seconds, and then prints out a wakeup
    * alarm message.
    */
  def run(args: List[String]): ZIO[ZEnv, Nothing, Int] =
    ???
}

object TicTacToe extends App {
  import zio.console._

  sealed trait Mark {
    final def renderChar: Char = this match {
      case Mark.X => 'X'
      case Mark.O => 'O'
    }
    final def render: String = renderChar.toString
  }
  object Mark {
    case object X extends Mark
    case object O extends Mark
  }

  final case class Board private (value: Vector[Vector[Option[Mark]]]) {
    final def place(row: Int, col: Int, mark: Mark): Option[Board] =
      if (row >= 0 && col >= 0 && row < 3 && col < 3)
        Some(copy(value = value.updated(row, value(row).updated(col, Some(mark)))))
      else None

    final def render =
      value.map(_.map(_.fold(" ")(_.render)).mkString(" ")).mkString("\n")

    final def won: Option[Mark] =
      if (wonBy(Mark.X)) Some(Mark.X)
      else if (wonBy(Mark.O)) Some(Mark.O)
      else None

    private final def wonBy(mark: Mark): Boolean =
      wonBy(0, 0, 1, 1, mark) ||
        wonBy(0, 2, 1, -1, mark) ||
        wonBy(0, 0, 0, 1, mark) ||
        wonBy(1, 0, 0, 1, mark) ||
        wonBy(2, 0, 0, 1, mark) ||
        wonBy(0, 0, 1, 0, mark) ||
        wonBy(0, 1, 1, 0, mark) ||
        wonBy(0, 2, 1, 0, mark)

    private final def wonBy(
        row0: Int,
        col0: Int,
        rowInc: Int,
        colInc: Int,
        mark: Mark
    ): Boolean =
      extractLine(row0, col0, rowInc, colInc).collect { case Some(v) => v }.toList == List(
        mark,
        mark,
        mark
      )

    private final def extractLine(
        row0: Int,
        col0: Int,
        rowInc: Int,
        colInc: Int
    ): Iterable[Option[Mark]] =
      for {
        row <- (row0 to (row0 + rowInc * 2))
        col <- (col0 to (col0 + colInc * 2))
      } yield value(row)(col)
  }
  object Board {
    final val empty = new Board(Vector.fill(3)(Vector.fill(3)(None)))

    def fromChars(
        first: Iterable[Char],
        second: Iterable[Char],
        third: Iterable[Char]
    ): Option[Board] =
      if (first.size != 3 || second.size != 3 || third.size != 3) None
      else {
        def toMark(char: Char): Option[Mark] =
          if (char.toLower == 'x') Some(Mark.X)
          else if (char.toLower == 'o') Some(Mark.O)
          else None

        Some(
          new Board(
            Vector(
              first.map(toMark).toVector,
              second.map(toMark).toVector,
              third.map(toMark).toVector
            )
          )
        )
      }
  }

  def renderBoard(board: Board): ZIO[Console, Nothing, Unit] = {
    val output = board.value
      .map(_.map(_.fold(" ")(_.render)).mkString(" ", " | ", " "))
      .mkString("\n---|---|---\n")

    putStrLn(output)
  }

  def run(args: List[String]): ZIO[ZEnv, Nothing, Int] =
    renderBoard(
      Board
        .fromChars(
          List(' ', 'O', 'X'),
          List('O', 'X', 'O'),
          List('X', ' ', ' ')
        )
        .get
    ) as 0
}
