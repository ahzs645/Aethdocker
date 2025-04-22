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
  timestamp: string
  rawBC: number | null
  processedBC: number | null
}

export default defineComponent({
  name: 'TimeSeriesChart',

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
    const chartOptions = computed(() => ({
      chart: {
        type: 'line',
        zoomType: 'x'
      },
      title: {
        text: props.title
      },
      xAxis: {
        type: 'datetime',
        title: {
          text: 'Time'
        }
      },
      yAxis: {
        title: {
          text: 'BC (ng/m³)'
        }
      },
      tooltip: {
        shared: true,
        crosshairs: true,
        valueDecimals: 2
      },
      plotOptions: {
        line: {
          marker: {
            enabled: false
          }
        }
      },
      series: [
        {
          name: 'Raw BC',
          data: props.data.map(point => [
            new Date(point.timestamp).getTime(),
            point.rawBC
          ]),
          color: '#7cb5ec'
        },
        {
          name: 'Processed BC',
          data: props.data.map(point => [
            new Date(point.timestamp).getTime(),
            point.processedBC
          ]),
          color: '#434348'
        }
      ],
      credits: {
        enabled: false
      },
      legend: {
        enabled: true
      }
    }))

    return {
      chartOptions
    }
  }
})
</script>