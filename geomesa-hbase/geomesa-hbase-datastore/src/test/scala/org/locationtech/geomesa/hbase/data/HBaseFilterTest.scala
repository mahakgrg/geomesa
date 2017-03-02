/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.data

import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.{HBaseTestingUtility, TableName}
import org.apache.hadoop.hbase.client.{Connection, Result, Scan}
import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.Params._
import org.locationtech.geomesa.hbase.filters.{JLazyDensityFilter, JSimpleFeatureFilter}
import org.locationtech.geomesa.hbase.utils.BatchScan
import org.locationtech.geomesa.index.index.ClientSideFiltering.RowAndValue
import org.locationtech.geomesa.index.utils.ExplainString
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class HBaseFilterTest extends Specification with LazyLogging {

  sequential

  val cluster = new HBaseTestingUtility()
  var connection: Connection = null

  step {
    logger.info("Starting embedded hbase")
    cluster.startMiniCluster(1)
    connection = cluster.getConnection
    logger.info("Started")
  }

  "HBaseDataStore" should {
    "work with points" in {
      val typeName = "testpoints"

      val params = Map(ConnectionParam.getName -> connection, BigTableNameParam.getName -> "test_sft")
      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]

      ds.getSchema(typeName) must beNull

      ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,dtg:Date,*geom:Point:srid=4326"))

      val sft = ds.getSchema(typeName)

      sft must not(beNull)

      val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]

      val toAdd = (0 until 10).map { i =>
        val sf = new ScalaSimpleFeature(i.toString, sft)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute(0, s"name$i")
        sf.setAttribute(1, f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute(2, s"POINT(41 51)")
        sf
      }

      val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
      ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))


      println(s"List tables: ${connection.getAdmin.listTableNames().map { t => new String(t.getName)}.mkString(", ")}")

      val table: TableName = connection.getAdmin.listTableNames("test_sft_testpoints_z3")(0)
      val scanRange: Scan = new Scan()

      def rowAndValue(result: Result): RowAndValue = {
        val cell = result.rawCells()(0)
        RowAndValue(cell.getRowArray, cell.getRowOffset, cell.getRowLength,
          cell.getValueArray, cell.getValueOffset, cell.getValueLength)
      }
      val deserializer = new KryoFeatureSerializer(sft, SerializationOptions.withoutId)

      import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
      def getIdFromRow(sft: SimpleFeatureType): (Array[Byte], Int, Int) => String = {
        val start = if (sft.isTableSharing) { 12 } else { 11 } // table sharing + shard + 2 byte short + 8 byte long
        (row, offset, length) => new String(row, offset + start, length - start, StandardCharsets.UTF_8)
      }
      val getId = getIdFromRow(sft)

      // 1. Goal scan one of the tables with data
      val scan = new BatchScan(connection, table, Seq(scanRange), 2, 100000)
      val results = scan.map { result =>
        val RowAndValue(row, rowOffset, rowLength, value, valueOffset, valueLength) = rowAndValue(result)
        val sf: SimpleFeature = deserializer.deserialize(value, valueOffset, valueLength)
        sf
      }
      results.foreach { f => println(s"ID: ${f.getID} name: ${f.getAttribute("name")}") }

      // 2. Scan the table with the JSimpleFeatureFilter
      //val cql = ECQL.toFilter("name = 'name5'")
      val filter = new JLazyDensityFilter(sft, null)
      //val filter = new JSimpleFeatureFilter(sft, cql)
      println(" ** Starting second scan ** ")

      val scan2 = new BatchScan(connection, table, Seq(scanRange), 2, 100000, Seq(filter))
      val foo = scan2.hasNext
      val results2 = scan2.map { result =>
        val RowAndValue(row, rowOffset, rowLength, value, valueOffset, valueLength) = rowAndValue(result)
        val sf: SimpleFeature = deserializer.deserialize(value, valueOffset, valueLength)
        sf
      }
      results2.foreach { f => println(s"ID: ${f.getID} name: ${f.getAttribute("name")}") }
      println(JLazyDensityFilter.getDensityResult)

      true mustEqual(true)
    }
  }

  step {
    logger.info("Stopping embedded hbase")
    cluster.shutdownMiniCluster()
    logger.info("Stopped")
  }
}
