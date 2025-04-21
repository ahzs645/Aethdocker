import { createApp } from 'vue'
import { createVuetify } from 'vuetify'
import 'vuetify/styles'
import App from './App.vue'
import router from './router'
import HighchartsVue from 'highcharts-vue'
import Highcharts from 'highcharts'
import HighchartsMore from 'highcharts/highcharts-more'
import HighchartsExporting from 'highcharts/modules/exporting'
import { registerAllModules } from 'handsontable/registry'
import 'leaflet/dist/leaflet.css'

// Initialize Highcharts modules
HighchartsMore(Highcharts)
HighchartsExporting(Highcharts)

// Initialize Handsontable
registerAllModules()

const vuetify = createVuetify()

const app = createApp(App)
app.use(router)
app.use(vuetify)
app.use(HighchartsVue)
app.mount('#app')