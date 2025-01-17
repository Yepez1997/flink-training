/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.ridesandfares.scala

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.functions.sink.{PrintSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.training.exercises.common.datatypes.{RideAndFare, TaxiFare, TaxiRide}
import org.apache.flink.training.exercises.common.sources.{TaxiFareGenerator, TaxiRideGenerator}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.util.Collector

/** The Stateful Enrichment exercise from the Flink training.
  *
  * The goal for this exercise is to enrich TaxiRides with fare information.
  */
object RidesAndFaresExercise {

  class RidesAndFaresJob(
      rideSource: SourceFunction[TaxiRide],
      fareSource: SourceFunction[TaxiFare],
      sink: SinkFunction[RideAndFare]
  ) {

    def execute(): JobExecutionResult = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment

      // rides source
      val rides = env
        .addSource(rideSource)
        .filter { ride =>
          ride.isStart
        }
        .keyBy { ride =>
          ride.rideId
        }

      // fares source
      val fares = env
        .addSource(fareSource)
        .keyBy { fare =>
          fare.rideId
        }

      // connect rides and fares based on fare id
      rides
        .connect(fares)
        .flatMap(new EnrichmentSolutionFunction())
        .addSink(sink)

      env.execute()
    }
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val job =
      new RidesAndFaresJob(new TaxiRideGenerator, new TaxiFareGenerator, new PrintSinkFunction)

    job.execute()
  }

  // RichCoFlatMapFunction may be time independent, in this case either ride state
  // or fare state can come in any order, but both events must exists before outputting.
  class EnrichmentSolutionFunction() extends RichCoFlatMapFunction[TaxiRide, TaxiFare, RideAndFare] {

    @transient private var rideState: ValueState[TaxiRide] = _
    @transient private var fareState: ValueState[TaxiFare] = _

    // open and setup state
    override def open(parameters: Configuration): Unit = {
      rideState = getRuntimeContext.getState(
        new ValueStateDescriptor[TaxiRide]("saved ride", classOf[TaxiRide])
      )
      fareState = getRuntimeContext.getState(
        new ValueStateDescriptor[TaxiFare]("saved fare", classOf[TaxiFare])
      )
    }

    // first input event
    override def flatMap1(ride: TaxiRide, out: Collector[RideAndFare]): Unit = {
      val fare = Option(fareState.value)
      fare match {
        case Some(f) => {
          fareState.clear()
          out.collect(new RideAndFare(ride, f))
        }
        case None =>
          rideState.update(ride)
      }
    }

    // second input event
    override def flatMap2(fare: TaxiFare, out: Collector[RideAndFare]): Unit = {
      val ride = Option(rideState.value)
      ride match {
        case Some(r) => {
          rideState.clear()
          out.collect(new RideAndFare(r, fare))
        }
        case None =>
          fareState.update(fare)
      }
    }
  }


  class EnrichmentFunction() extends RichCoFlatMapFunction[TaxiRide, TaxiFare, RideAndFare] {

    private var rideState: ValueState[RideAndFare] = _

    override def open(parameters: Configuration): Unit = {
      rideState = getRuntimeContext.getState(
        new ValueStateDescriptor[RideAndFare]("ride", createTypeInformation[RideAndFare])
      )
    }

    override def flatMap1(ride: TaxiRide, out: Collector[RideAndFare]): Unit = {
      val tempCurrentRide = rideState.value()

      val currentRide = if (tempCurrentRide != null) {
        tempCurrentRide
      } else {
        new RideAndFare(null, null)
      }

      // set whenever received, edge case is to check if already set - for idempotency
      if (currentRide.getTaxiRide == null) {
        currentRide.setTaxiRide(ride)
        rideState.update(currentRide)
      }

      if (currentRide.getTaxiFare != null && currentRide.getTaxiRide != null) {
        // collect if both values set
        out.collect(currentRide)
        rideState.clear()
      }

    }

    override def flatMap2(fare: TaxiFare, out: Collector[RideAndFare]): Unit = {
      val tempCurrentRide = rideState.value()

      val currentRide = if (tempCurrentRide != null) {
        tempCurrentRide
      } else {
        new RideAndFare(null, null)
      }

      if (currentRide.getTaxiFare == null) {
        currentRide.setTaxiFare(fare)
        rideState.update(currentRide)
      }

      if (currentRide.getTaxiFare != null && currentRide.getTaxiRide != null) {
        out.collect(currentRide)
        rideState.clear()
      }

    }
  }



}
