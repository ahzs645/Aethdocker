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
  [key: string]: {
    data: Array<{
      [key: string]: number | null
      processedBC: number | null
    }>,
    correlation: {
      pearson_r: number
      pearson_p: number
      spearman_r: number
      spearman_p: number
    } | null
  }
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
      type: Object as PropType<WeatherData>,
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
      const paramMap = {
        'temperature_c': { label: 'Temperature', unit: '°C' },
        'relative_humidity_percent': { label: 'Relative Humidity', unit: '%' },
        'wind_speed_kmh': { label: 'Wind Speed', unit: 'km/h' },
        'pressure_hpa': { label: 'Pressure', unit: 'hPa' }
      }

      Object.keys(props.data).forEach(key => {
        if (key in paramMap) {
          params.push({
            key,
            label: paramMap[key as keyof typeof paramMap].label,
            unit: paramMap[key as keyof typeof paramMap].unit
          })
        }
      })

      return params
    })

    const getChartOptions = (param: WeatherParam) => {
      const weatherData = props.data[param.key]
      if (!weatherData?.data) return null

      const validData = weatherData.data.filter(point =>
        point[param.key] !== null &&
        point.processedBC !== null
      )

      const xValues = validData.map(point => point[param.key] as number)
      const yValues = validData.map(point => point.processedBC as number)
      const correlationStats = weatherData.correlation

      if (validData.length < 2) return null

      const minX = Math.min(...xValues)
      const maxX = Math.max(...xValues)
      const minY = Math.min(...yValues)
      const maxY = Math.max(...yValues)

      // Calculate trend line using Pearson correlation if available
      let trendLineData: [number, number][] = []
      if (correlationStats?.pearson_r) {
        const xMean = xValues.reduce((a, b) => a + b, 0) / xValues.length
        const yMean = yValues.reduce((a, b) => a + b, 0) / yValues.length
        const slope = correlationStats.pearson_r *
          (Math.sqrt(yValues.reduce((a, b) => a + Math.pow(b - yMean, 2), 0) / yValues.length) /
           Math.sqrt(xValues.reduce((a, b) => a + Math.pow(b - xMean, 2), 0) / xValues.length))
        const intercept = yMean - slope * xMean
        trendLineData = [
          [minX, slope * minX + intercept],
          [maxX, slope * maxX + intercept]
        ]
      }

      const correlationText = correlationStats
        ? `Pearson R: ${correlationStats.pearson_r.toFixed(3)} (p=${correlationStats.pearson_p.toFixed(3)})\nSpearman R: ${correlationStats.spearman_r.toFixed(3)} (p=${correlationStats.spearman_p.toFixed(3)})`
        : ''

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
          ...(trendLineData.length ? [{
            name: 'Trend Line',
            type: 'line',
            data: trendLineData,
            color: '#434348',
            dashStyle: 'dash',
            marker: {
              enabled: false
            }
          }] : [])
        ],
        annotations: correlationText ? [{
          labels: [{
            point: {
              x: minX + (maxX - minX) * 0.1,
              y: maxY - (maxY - minY) * 0.1
            },
            text: correlationText,
            style: {
              fontSize: '11px'
            }
          }]
        }] : [],
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