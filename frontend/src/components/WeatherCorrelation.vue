<template>
  <v-card>
    <v-card-title>{{ title }}</v-card-title>
    <v-card-text>
      <v-row>
        <v-col v-for="param in weatherParams" :key="param.key" cols="12" md="6">
          <highcharts
            :options="getChartOptions(param)"
            :updateArgs="[true, true, true]"
          ></highcharts>
        </v-col>
      </v-row>
    </v-card-text>
  </v-card>
</template>

<script lang="ts">
import { defineComponent, computed, PropType } from 'vue'
import Highcharts from 'highcharts'

interface WeatherData {
  processedBC: number
  temperature_c?: number
  relative_humidity_percent?: number
  wind_speed_kmh?: number
  pressure_hpa?: number
  [key: string]: number | undefined
}

interface WeatherParam {
  key: string
  label: string
  unit: string
}

export default defineComponent({
  name: 'WeatherCorrelation',

  props: {
    data: {
      type: Array as PropType<WeatherData[]>,
      required: true
    },
    title: {
      type: String,
      required: true
    }
  },

  setup(props) {
    const weatherParams = computed(() => {
      const params: WeatherParam[] = []
      const sample = props.data[0] || {}

      if ('temperature_c' in sample) {
        params.push({ key: 'temperature_c', label: 'Temperature', unit: '°C' })
      }
      if ('relative_humidity_percent' in sample) {
        params.push({ key: 'relative_humidity_percent', label: 'Relative Humidity', unit: '%' })
      }
      if ('wind_speed_kmh' in sample) {
        params.push({ key: 'wind_speed_kmh', label: 'Wind Speed', unit: 'km/h' })
      }
      if ('pressure_hpa' in sample) {
        params.push({ key: 'pressure_hpa', label: 'Pressure', unit: 'hPa' })
      }

      return params
    })

    const calculateCorrelation = (xValues: number[], yValues: number[]) => {
      const n = xValues.length
      let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0, sumY2 = 0

      for (let i = 0; i < n; i++) {
        sumX += xValues[i]
        sumY += yValues[i]
        sumXY += xValues[i] * yValues[i]
        sumX2 += xValues[i] * xValues[i]
        sumY2 += yValues[i] * yValues[i]
      }

      const numerator = n * sumXY - sumX * sumY
      const denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY))
      
      return denominator === 0 ? 0 : numerator / denominator
    }

    const getChartOptions = (param: WeatherParam) => {
      const validData = props.data.filter(point => 
        point[param.key] !== undefined && 
        point.processedBC !== undefined
      )

      const xValues = validData.map(point => point[param.key] as number)
      const yValues = validData.map(point => point.processedBC)
      const correlation = calculateCorrelation(xValues, yValues)

      // Calculate trend line
      const xSum = xValues.reduce((a, b) => a + b, 0)
      const ySum = yValues.reduce((a, b) => a + b, 0)
      const n = xValues.length
      const xMean = xSum / n
      const yMean = ySum / n
      const ssxx = xValues.reduce((a, b) => a + Math.pow(b - xMean, 2), 0)
      const ssxy = xValues.reduce((a, b, i) => a + (b - xMean) * (yValues[i] - yMean), 0)
      const slope = ssxy / ssxx
      const intercept = yMean - slope * xMean

      const minX = Math.min(...xValues)
      const maxX = Math.max(...xValues)
      const trendLineData = [
        [minX, slope * minX + intercept],
        [maxX, slope * maxX + intercept]
      ]

      return {
        chart: {
          type: 'scatter',
          zoomType: 'xy'
        },
        title: {
          text: `BC vs ${param.label}`
        },
        xAxis: {
          title: {
            text: `${param.label} (${param.unit})`
          }
        },
        yAxis: {
          title: {
            text: 'BC (ng/m³)'
          }
        },
        tooltip: {
          formatter: function(this: Highcharts.TooltipFormatterContextObject) {
            return `${param.label}: ${this.x?.toFixed(2)} ${param.unit}<br>BC: ${this.y?.toFixed(2)} ng/m³`
          }
        },
        plotOptions: {
          scatter: {
            marker: {
              radius: 4
            }
          }
        },
        series: [
          {
            name: 'Data Points',
            data: validData.map(point => [point[param.key], point.processedBC]),
            color: '#7cb5ec'
          },
          {
            name: 'Trend Line',
            type: 'line',
            data: trendLineData,
            color: '#434348',
            dashStyle: 'dash',
            marker: {
              enabled: false
            }
          }
        ],
        annotations: [{
          labels: [{
            point: {
              x: minX + (maxX - minX) * 0.1,
              y: Math.max(...yValues) - (Math.max(...yValues) - Math.min(...yValues)) * 0.1
            },
            text: `Correlation: ${correlation.toFixed(3)}`
          }]
        }],
        credits: {
          enabled: false
        }
      }
    }

    return {
      weatherParams,
      getChartOptions
    }
  }
})
</script>