<template>
  <div class="bottomBar">
    <router-view />
    <van-tabbar route>
      <van-tabbar-item icon="home-o" replace to="/recLists">้ฆ้กต</van-tabbar-item>
      <van-tabbar-item icon="friends-o" replace to="/Myself">ๆ็</van-tabbar-item>
    </van-tabbar>
  </div>
</template>

<script>
  export default {
    name: 'bottomBar',
    data() {
      return {
      }
    },
    methods: {
    },
  }
</script>

<style scoped>
  .bottomBar {
    position: fixed;
    bottom: 0px;
    width: 100vw;
  }
</style>