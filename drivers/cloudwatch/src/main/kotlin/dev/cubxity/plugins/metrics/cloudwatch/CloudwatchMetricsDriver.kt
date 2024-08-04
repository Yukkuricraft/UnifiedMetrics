/*
 *     This file is part of UnifiedMetrics.
 *
 *     UnifiedMetrics is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     UnifiedMetrics is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General Public License
 *     along with UnifiedMetrics.  If not, see <https://www.gnu.org/licenses/>.
 */

package dev.cubxity.plugins.metrics.cloudwatch

import dev.cubxity.plugins.metrics.api.UnifiedMetrics
import dev.cubxity.plugins.metrics.api.metric.MetricsDriver
import dev.cubxity.plugins.metrics.api.metric.data.CounterMetric
import dev.cubxity.plugins.metrics.api.metric.data.GaugeMetric
import dev.cubxity.plugins.metrics.api.metric.data.HistogramMetric
import dev.cubxity.plugins.metrics.api.metric.data.Metric
import dev.cubxity.plugins.metrics.api.util.fastForEach
import dev.cubxity.plugins.metrics.api.util.toGoString
import dev.cubxity.plugins.metrics.cloudwatch.config.CloudwatchConfig

import kotlinx.coroutines.*
import kotlin.math.max
import kotlin.system.measureTimeMillis

import aws.sdk.kotlin.runtime.auth.credentials.ProfileCredentialsProvider
import aws.sdk.kotlin.services.cloudwatch.CloudWatchClient
import aws.sdk.kotlin.services.cloudwatch.model.Dimension
import aws.sdk.kotlin.services.cloudwatch.model.MetricDatum
import aws.sdk.kotlin.services.cloudwatch.model.PutMetricDataRequest
import aws.sdk.kotlin.services.cloudwatch.model.StandardUnit

import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

class CloudwatchMetricsDriver(private val api: UnifiedMetrics, private val config: CloudwatchConfig) : MetricsDriver {
    private val coroutineScope = CoroutineScope(Dispatchers.Default) + SupervisorJob()

    override fun initialize() {
        scheduleTasks()
    }

    fun createCloudwatchClient(): CloudWatchClient {
        return CloudWatchClient {
            region = config.authentication.regionName
            credentialsProvider = ProfileCredentialsProvider(profileName = config.authentication.awsProfileName)
        }
    }

    override fun close() {
        println("Calling CloudwatchMetricsDriver.close()")
        coroutineScope.cancel()
    }

    private fun scheduleTasks() {
        val interval = config.pushIntervalSeconds * 1000

        coroutineScope.launch {
            while (true) {
                val time = measureTimeMillis {
                    try {
                        val metrics = api.metricsManager.collect()
                        writeMetrics(metrics)
                    } catch (error: Throwable) {
                        api.logger.severe("An error occurred whilst writing samples to Cloudwatch", error)
                    }
                }
                delay(max(0, interval - time))
            }
        }
    }

    private suspend fun writeMetrics(metrics: List<Metric>) {
        val maxCloudwatchDatapointsPerReq = 150

        val baseDimensions: MutableList<Dimension> = ArrayList()
        baseDimensions.add(Dimension {
            name = "server"
            value = api.serverName
        })

        val metricDataList = ArrayList<MetricDatum>()

        metrics.fastForEach { metric ->
            val dimensionList = baseDimensions.toMutableList()
            metric.labels.forEach { label ->
                dimensionList.add(Dimension {
                    name = label.key
                    value = label.value
                })
            }

            var metricValue: Double = 0.0
            when (metric) {
                is GaugeMetric -> metricValue = metric.value
                is CounterMetric -> metricValue = metric.value
                is HistogramMetric -> {
//                    println("!!!! Cloudwatch does not support Histogram metrics!")
//                    println(">> metric.name: ${metric.name}")
//                    dimensionList.forEach { dimension ->
//                        println(">> dimension.key - ${dimension.name}")
//                        println(">> dimension.value - ${dimension.value}")
//                    }
//                    println(">> Sample count: ${metric.sampleCount}")
//                    println(">> Sample sum: ${metric.sampleSum}")
//                    metric.bucket.forEach { bucket ->
//                        println(">> Bucket upperBound: ${bucket.upperBound}")
//                        println(">> Bucket cumulativeCount: ${bucket.cumulativeCount}")
//                    }
                    return@fastForEach
                }
            }

            val time = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT)
            val instant = Instant.parse(time)
//            println("+++++")
//            println("[Debug] metric.name - ${metric.name}")
//            println("[Debug] metric.value - ${metricValue}")
//            dimensionList.forEach { dimension ->
//                println("[Debug] dimension.key - ${dimension.name}")
//                println("[Debug] dimension.value - ${dimension.value}")
//            }
            metricDataList.add(
                MetricDatum {
                    metricName = metric.name
                    unit = StandardUnit.None
                    value = metricValue
                    dimensions = dimensionList
                    timestamp =
                        aws.smithy.kotlin.runtime.time
                            .Instant(instant)
                }
            )
        }

        metricDataList.chunked(maxCloudwatchDatapointsPerReq).fastForEach { chunk ->
            val request =
                PutMetricDataRequest {
                    namespace = config.awsMetricNamespace
                    metricData = chunk
                }

            createCloudwatchClient().use { cwClient ->
                val cwClient = cwClient ?: return

//                println("Sending request to CW:")
//                println("namespace: ${request.namespace}")
//                println("metricData: ${request.metricData}")

                cwClient.putMetricData(request)
//                println("Added metric values for for namespace $customMetricNamespace")
            }
        }
    }
}
