/*

    adam-dishevelled-bio  dishevelled-bio on Spark via ADAM pipe APIs.
    Copyright (c) 2016 held jointly by the individual authors.

    This library is free software; you can redistribute it and/or modify it
    under the terms of the GNU Lesser General Public License as published
    by the Free Software Foundation; either version 3 of the License, or (at
    your option) any later version.

    This library is distributed in the hope that it will be useful, but WITHOUT
    ANY WARRANTY; with out even the implied warranty of MERCHANTABILITY or
    FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
    License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this library;  if not, write to the Free Software Foundation,
    Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA.

    > http://www.fsf.org/licensing/licenses/lgpl.html
    > http://www.opensource.org/licenses/lgpl-license.php

*/
package com.github.heuermh.adam.dishevelledbio

import org.apache.spark.SparkContext
import org.bdgenomics.adam.models.VariantContext
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.variation.{ VariantContextRDD, VCFInFormatter, VCFOutFormatter }
import org.bdgenomics.utils.cli._
import org.bdgenomics.utils.misc.Logging
import org.kohsuke.args4j.{ Option => Args4jOption, Argument }

class AdamDshFilterVcfArgs extends ParquetArgs {
  @Argument(required = true, metaVar = "INPUT", usage = "input VCF file", index = 0)
  var inputVcfFile: String = _

  @Argument(required = true, metaVar = "OUTPUT", usage = "output VCF file", index = 1)
  var outputVcfFile: String = null

  @Args4jOption(required = false, name = "--id", usage = "filter by id, specify as id1,id2,id3")
  var id: String = null

  @Args4jOption(required = false, name = "--range", usage = "filter by range, specify as chrom:start-end in 0-based coordinates")
  var range: String = null

  @Args4jOption(required = false, name = "--qual", usage = "filter by quality score")
  var qual: String = null

  @Args4jOption(required = false, name =" --filter", usage = "filter to records that have passed all filters")
  var filter: String = null

  @Args4jOption(required = false, name = "--single", usage = "save OUTPUT as single file, default false")
  var asSingleFile: Boolean = false
}

object AdamDshFilterVcf extends BDGCommandCompanion {
  override val commandName = "adam-dsh-filter-vcf"
  override val commandDescription = "dsh-filter-vcf on Spark via ADAM pipe APIs"

  override def apply(cmdLine: Array[String]): AdamDshFilterVcf =
    new AdamDshFilterVcf(Args4j[AdamDshFilterVcfArgs](cmdLine))
}

/**
 * dsh-filter-vcf on Spark via ADAM pipe APIs.
 * 
 * @author  Michael Heuer
  */
class AdamDshFilterVcf(val args: AdamDshFilterVcfArgs) extends BDGSparkCommand[AdamDshFilterVcfArgs] with Logging {
  override val companion = AdamDshFilterVcf

  override def run(sc: SparkContext): Unit = {
    val input: VariantContextRDD = sc.loadVcf(args.inputVcfFile)

    log.warn("Loaded %d variant context records".format(input.rdd.count()))

    implicit val tFormatter = VCFInFormatter
    implicit val uFormatter = new VCFOutFormatter

    // reassemble command from our arguments
    val dshFilterVcfCommand = StringBuilder.newBuilder
    dshFilterVcfCommand.append("dsh-filter-vcf")
    Option(args.id).foreach(id => dshFilterVcfCommand.append(" --id " + id))
    Option(args.range).foreach(range => dshFilterVcfCommand.append(" --range " + range))
    Option(args.qual).foreach(qual => dshFilterVcfCommand.append(" --qual " + qual))
    Option(args.filter).foreach(filter => dshFilterVcfCommand.append(" --filter " + filter))

    val output: VariantContextRDD = input.pipe[VariantContext, VariantContextRDD, VCFInFormatter](dshFilterVcfCommand.toString)
      .transform(_.cache())

    log.warn("Saving %d variant context records after filtering".format(output.rdd.count()))

    output.saveAsVcf(args.outputVcfFile, args.asSingleFile)
  }
}
