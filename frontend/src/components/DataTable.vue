<template>
  <v-card>
    <v-card-title>{{ title }}</v-card-title>
    <v-card-text>
      <div class="hot-container">
        <hot-table
          :settings="hotSettings"
          :data="data"
          licenseKey="non-commercial-and-evaluation"
        ></hot-table>
      </div>
    </v-card-text>
  </v-card>
</template>

<script lang="ts">
import { defineComponent, computed, PropType } from 'vue'
import { HotTable } from '@handsontable/vue3'
import { registerAllModules } from 'handsontable/registry'
import 'handsontable/dist/handsontable.full.css'

registerAllModules()

interface DataPoint {
  timestamp: string
  rawBC: number
  processedBC: number
  atn: number
  [key: string]: any
}

export default defineComponent({
  name: 'DataTable',

  components: {
    HotTable
  },

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
    const columns = computed(() => {
      if (props.data.length === 0) return []
      
      return Object.keys(props.data[0]).map(key => ({
        data: key,
        title: key.charAt(0).toUpperCase() + key.slice(1),
        type: key === 'timestamp' ? 'text' : 'numeric',
        readOnly: true,
        numericFormat: key !== 'timestamp' ? {
          pattern: '0.00'
        } : undefined
      }))
    })

    const hotSettings = computed(() => ({
      columns: columns.value,
      colHeaders: true,
      rowHeaders: true,
      height: 400,
      width: '100%',
      stretchH: 'all',
      manualColumnResize: true,
      manualRowResize: true,
      filters: true,
      dropdownMenu: true,
      contextMenu: true,
      multiColumnSorting: true,
      readOnly: true
    }))

    return {
      hotSettings
    }
  }
})
</script>

<style scoped>
.hot-container {
  width: 100%;
  height: 400px;
  overflow: hidden;
}
</style>