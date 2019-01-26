package sparkproject.utils

import scopt.{DefaultOParserSetup, OParser, OParserSetup}
import sparkproject.{Config, Constants}

object ArgsParser {
  def parse(args: Array[String]): Config = {
    val setup: OParserSetup = new DefaultOParserSetup {
      override def showUsageOnError = Some(true)
    }
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName(Constants.projectName.toLowerCase().replace(" ", "")),
        head(Constants.projectName, "1.x"),
        help("help").abbr("h") text "Prints this usage text",
        cmd("train")
          .action((_, c) => c.copy(mode = "train"))
          .text("Train model/models")
          .children(
            opt[String]("input")
              .abbr("i").valueName("<dataset>")
              .required()
              .action((x, c) => c.copy(input = x))
              .text("Input dataset filepath"),
            opt[String]("export")
              .abbr("e").valueName("<path>")
              .action((x, c) => c.copy(export = x))
              .text("Export model path")
          ),
        cmd("predict")
          .action((_, c) => c.copy(mode = "predict"))
          .text("Predict with model from disk")
          .children(
            opt[Unit]("evaluate")
              .abbr("e")
              .action((_, c) => c.copy(eval = true))
              .text("With the --evaluate flag evaluation is done"),
            opt[String]("model")
              .abbr("m").valueName("<model>")
              .required()
              .action((x, c) => c.copy(model = x))
              .text("Model to import"),
            opt[String]("input")
              .abbr("i").valueName("<dataset>")
              .required()
              .action((x, c) => c.copy(input = x))
              .text("Input dataset path"),
            opt[String]("output")
              .abbr("o").valueName("<path>")
              .action((x, c) => c.copy(output = x))
              .text("Output dataset path"),
            checkConfig(c =>
              if (c.mode.isEmpty) failure("A command is required.") else success
            )
          )
      )
    }

    val conf = OParser.parse(parser, args, Config(), setup) match {
      case Some(config) =>
        config
      case _ =>
        sys.exit(1)
    }
    conf
  }
}
