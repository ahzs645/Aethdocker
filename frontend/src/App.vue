<template>
  <v-app>
    <v-app-bar app>
      <v-app-bar-title>Aethalometer Data Analysis</v-app-bar-title>
    </v-app-bar>

    <v-main>
      <v-container fluid>
        <router-view></router-view>
      </v-container>
    </v-main>
  </v-app>
</template>

<script lang="ts">
import { defineComponent } from 'vue'

export default defineComponent({
  name: 'App'
})
</script>

<style>
@import 'vuetify/styles';
</style>