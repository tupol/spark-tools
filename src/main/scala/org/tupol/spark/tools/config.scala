package org.tupol.spark.tools

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigResolveOptions}

import scala.util.Try

object config {

  def getApplicationConfiguration(args: Array[String], configurationFileName: String): Try[Config] =
    for {
      argsConfig <- Try(ConfigFactory.parseString(args.mkString("\n")))
      resolvedConfig <- resolveConfig(argsConfig, configurationFileName)
    } yield resolvedConfig

  private def resolveConfig(inputConfig: Config, fallbackResource: String): Try[Config] =
    Try {
      val defaultConfig = ConfigFactory
        .load(
          ConfigParseOptions.defaults,
          ConfigResolveOptions.defaults.setAllowUnresolved(true)
        )
      val unresolvedConfig = ConfigFactory
        .load(
          fallbackResource,
          ConfigParseOptions.defaults,
          ConfigResolveOptions.defaults.setAllowUnresolved(true)
        )
      inputConfig
        .withFallback(unresolvedConfig)
        .withFallback(defaultConfig)
        .resolve()
    }
}
