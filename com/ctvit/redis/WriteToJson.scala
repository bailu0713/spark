package com.ctvit.redis

import net.sf.json.JSONObject

/**
 * Created by BaiLu on 2015/10/13.
 */
object WriteToJson {

  def main(args: Array[String]) {

      val programRecommend: ChannelRecommend = new ChannelRecommend
      programRecommend.setChannelId("27747")
      programRecommend.setChannelname("北京卫视高清")
      programRecommend.setChannelpic("/channel/image/BTVHD_888_666.jpg;/channel/image/BTVHD_684_513.jpg;/channel/image/BTVHD_432_324.jpg")
      programRecommend.setRank("1")
       println(JSONObject.fromObject(programRecommend).toString)
  }

}

class ChannelRecommend {
  def getChannelId: String = {
    return channelId
  }

  def setChannelId(channelId: String) {
    this.channelId = channelId
  }

  def getChannelname: String = {
    return channelname
  }

  def setChannelname(channelname: String) {
    this.channelname = channelname
  }

  def getChannelpic: String = {
    return channelpic
  }

  def setChannelpic(channelpic: String) {
    this.channelpic = channelpic
  }

  def getRank: String = {
    return rank
  }

  def setRank(rank: String) {
    this.rank = rank
  }

  private var channelId: String = null
  private var channelname: String = null
  private var channelpic: String = null
  private var rank: String = null
}