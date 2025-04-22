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
  rawBC: number | null
  processedBC: number | null
}

interface ComparisonStats {
  pearson_r: number
  pearson_p: number
  spearman_r: number
  spearman_p: number
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
    },
    stats: {
      type: Object as PropType<ComparisonStats>,
      default: null
    }
  },

  setup(props) {

    const chartOptions = computed(() => {
      const validData = props.data.filter(
        point => point.rawBC !== null && point.processedBC !== null
      ) as { rawBC: number; processedBC: number }[]

      if (validData.length === 0) return null

      const minValue = Math.min(
        ...validData.map(point => Math.min(point.rawBC, point.processedBC))
      )
      const maxValue = Math.max(
        ...validData.map(point => Math.max(point.rawBC, point.processedBC))
      )

      const correlationText = props.stats
        ? `Pearson R: ${props.stats.pearson_r.toFixed(3)} (p=${props.stats.pearson_p.toFixed(3)})\nSpearman R: ${props.stats.spearman_r.toFixed(3)} (p=${props.stats.spearman_p.toFixed(3)})`
        : ''

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
            data: validData.map(point => [point.rawBC, point.processedBC]),
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
        annotations: correlationText ? [{
          labels: [{
            point: {
              x: minValue + (maxValue - minValue) * 0.1,
              y: maxValue - (maxValue - minValue) * 0.1
            },
            text: correlationText
          }]
        }] : [],
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