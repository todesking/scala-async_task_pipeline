package com.todesking.async_task_pipeline

import com.todesking.{async_task_pipeline => pipe}

object Main {
  def main(args:Array[String]):Unit = {
    /*
    val src:TraversableOnce[URL]

    val pipe = Pipeline.builder

    val filterByRule = Pipe.unordered[URL, URL] {url => if(canCrawl(url)) emit(url) }
    val filterByRobotsTxt = Pipe.unordered.grouped[URL, URL, Domain] {(domain, urls, out) =>
      def findRule(domain:Domain):RobotsTxt = {
        RobtsTxtCache.findOrRead(domain)
      }
      val robots = findRule(domain)
      urls.foreach(url => if(robots.canCrawl(url)) out.emit(url))
    }
    val throttle = Pipe.unordered.grouped[URL, URL, Domain] {(domain, urls) =>
      urls.foreach {url =>
        DomainStatus.waitUntilCrawlable(domain)
        emit(url)
      }
    }
    val crawl = Pipe.unordered.grouped[URL, PageContent, Domain] {(domain, urls, out) =>
      def httpGet(url) = { DomainStatus.crawling(domain) { rawGet(url) } }
      urls.foreach {url=> out.emit(new PageContent(httpGet(url))) }
    }
    val persistContent = Pipe.tap[PageContent](repository[PageContent].update(_))

    val analyze = Pipeline.unordered[PageContent, PageKeyword](analyze(_))

    val persistKeyword = Pipe.tap[PageKeyword](save(_))

    val pipeline:UnorderdPipeline[URL, PageContent] = filterByRule >> filterByRobotsTxt >> throttle >> crawl >> persistContent|(analyze >> persistKeyword)

    pipeline.emit(src)
    pipeline.await()
    */
  }
}


