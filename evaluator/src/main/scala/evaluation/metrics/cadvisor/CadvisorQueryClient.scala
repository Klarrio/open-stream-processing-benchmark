package evaluation.metrics.cadvisor

import evaluation.utils.QueryResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils

import scala.util.parsing.json.JSON

object CadvisorQueryClient {
  val client = new DefaultHttpClient

  def getResponse(url: String): Map[String, Any] = {
    val httpGet = new HttpGet(url)

    val response = client.execute(httpGet)

    val in = response.getEntity
    val encoding = response.getEntity.getContentEncoding

    val decodedResponse = EntityUtils.toString(in, "UTF-8")

    val jsonResponse = JSON.parseFull(decodedResponse).get.asInstanceOf[Map[String, Any]]
    jsonResponse
  }
}
