<template>
  <div>
    <v-row>
      <v-col cols="12">
        <v-card>
          <v-card-title>Data Upload</v-card-title>
          <v-card-text>
            <v-file-input
              v-model="aethalometerFile"
              label="Aethalometer Data (CSV)"
              accept=".csv"
              @change="handleAethalometerUpload"
            ></v-file-input>
            <v-file-input
              v-model="weatherFile"
              label="Weather Data (CSV) (Optional)"
              accept=".csv"
              @change="handleWeatherUpload"
            ></v-file-input>
            <v-select
              v-model="wavelength"
              :items="wavelengthOptions"
              label="Wavelength"
            ></v-select>
            <v-text-field
              v-model="atnMin"
              label="ATN Min"
              type="number"
              step="0.01"
              min="0.01"
            ></v-text-field>
            <v-btn
              color="primary"
              :loading="processing"
              :disabled="!aethalometerFile"
              @click="processData"
            >
              Process Data
            </v-btn>
          </v-card-text>
        </v-card>
      </v-col>
    </v-row>

    <v-row v-if="results">
      <v-col cols="12" v-if="results.visualization_data?.time_series_data">
        <time-series-chart
          :data="results.visualization_data.time_series_data"
          title="BC Time Series"
        />
      </v-col>
      <v-col cols="12" v-if="results.visualization_data?.comparison_data">
        <comparison-chart
          :data="results.visualization_data.comparison_data"
          :stats="results.visualization_data.comparison_stats"
          title="BC Comparison"
        />
      </v-col>
      <v-col cols="12" v-if="results.visualization_data?.weather_correlation_data">
        <weather-correlation
          :data="results.visualization_data.weather_correlation_data"
          title="Weather Correlation"
        />
      </v-col>
      <v-col cols="12">
        <data-table
          :data="results.processed_data"
          title="Processed Data"
        />
      </v-col>
    </v-row>
  </div>
</template>

<script lang="ts">
import { defineComponent, ref } from 'vue'
import TimeSeriesChart from '../components/TimeSeriesChart.vue'
import ComparisonChart from '../components/ComparisonChart.vue'
import WeatherCorrelation from '../components/WeatherCorrelation.vue'
import DataTable from '../components/DataTable.vue'
import axios from 'axios'

export default defineComponent({
  name: 'Dashboard',
  
  components: {
    TimeSeriesChart,
    ComparisonChart,
    WeatherCorrelation,
    DataTable
  },

  setup() {
    const aethalometerFile = ref<File | null>(null)
    const weatherFile = ref<File | null>(null)
    const wavelength = ref('Blue')
    const atnMin = ref(0.01)
    const processing = ref(false)
    const results = ref(null)
    const jobId = ref<string | null>(null)

    const wavelengthOptions = [
      'Blue',
      'Green',
      'Red',
      'UV',
      'IR'
    ]

    const processData = async () => {
      if (!aethalometerFile.value) return

      processing.value = true
      const formData = new FormData()
      formData.append('aethalometer_file', aethalometerFile.value)
      if (weatherFile.value) {
        formData.append('weather_file', weatherFile.value)
      }
      formData.append('wavelength', wavelength.value)
      formData.append('atn_min', atnMin.value.toString())

      try {
        const response = await axios.post('/api/process', formData)
        jobId.value = response.data.job_id
        await pollJobStatus()
      } catch (error) {
        console.error('Error processing data:', error)
      } finally {
        processing.value = false
      }
    }

    const pollJobStatus = async () => {
      if (!jobId.value) return

      while (true) {
        try {
          const response = await axios.get(`/api/status/${jobId.value}`)
          if (response.data.status === 'Completed') {
            results.value = response.data.results
            break
          } else if (response.data.status === 'Error') {
            console.error('Processing error:', response.data.error)
            break
          }
          await new Promise(resolve => setTimeout(resolve, 1000))
        } catch (error) {
          console.error('Error polling status:', error)
          break
        }
      }
    }

    return {
      aethalometerFile,
      weatherFile,
      wavelength,
      wavelengthOptions,
      atnMin,
      processing,
      results,
      processData
    }
  }
})
</script>