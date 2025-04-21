<template>
  <v-card>
    <v-card-title>{{ title }}</v-card-title>
    <v-card-text>
      <highcharts
        :options="chartOptions"
        :updateArgs="[true, true, true]"
      ></highcharts>
    </v-card-text>
  </v-card>
</template>

<script lang="ts">
import { defineComponent, computed, PropType } from 'vue'
import Highcharts from 'highcharts'

interface DataPoint {
  rawBC: number
  processedBC: number
}

export default defineComponent({
  name: 'ComparisonChart',

  props: {
    data: {
      type: Array as PropType<DataPoint[]>,
      required: true
    },
    title: {
      type: String,
      required: true
    }
  },

  setup(props) {
    const calculateCorrelation = (data: DataPoint[]) => {
      const n = data.length
      let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0, sumY2 = 0

      data.forEach(point => {
        sumX += point.rawBC
        sumY += point.processedBC
        sumXY += point.rawBC * point.processedBC
        sumX2 += point.rawBC * point.rawBC
        sumY2 += point.processedBC * point.processedBC
      })

      const numerator = n * sumXY - sumX * sumY
      const denominator = Math.sqrt((n * sumX2 - sumX * sumX) * (n * sumY2 - sumY * sumY))
      
      return denominator === 0 ? 0 : numerator / denominator
    }

    const chartOptions = computed(() => {
      const correlation = calculateCorrelation(props.data)
      const minValue = Math.min(
        ...props.data.map(point => Math.min(point.rawBC, point.processedBC))
      )
      const maxValue = Math.max(
        ...props.data.map(point => Math.max(point.rawBC, point.processedBC))
      )

      return {
        chart: {
          type: 'scatter',
          zoomType: 'xy'
        },
        title: {
          text: props.title
        },
        xAxis: {
          title: {
            text: 'Raw BC (ng/m³)'
          },
          min: minValue,
          max: maxValue
        },
        yAxis: {
          title: {
            text: 'Processed BC (ng/m³)'
          },
          min: minValue,
          max: maxValue
        },
        tooltip: {
          formatter: function(this: Highcharts.TooltipFormatterContextObject) {
            return `Raw BC: ${this.x?.toFixed(2)} ng/m³<br>Processed BC: ${this.y?.toFixed(2)} ng/m³`
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
            name: 'BC Comparison',
            data: props.data.map(point => [point.rawBC, point.processedBC]),
            color: '#7cb5ec'
          },
          {
            name: '1:1 Line',
            type: 'line',
            data: [[minValue, minValue], [maxValue, maxValue]],
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
              x: minValue + (maxValue - minValue) * 0.1,
              y: maxValue - (maxValue - minValue) * 0.1
            },
            text: `Correlation: ${correlation.toFixed(3)}`
          }]
        }],
        credits: {
          enabled: false
        }
      }
    })

    return {
      chartOptions
    }
  }
})
</script>