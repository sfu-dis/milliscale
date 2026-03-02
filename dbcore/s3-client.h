/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#pragma once

#include <aws/core/Core_EXPORTS.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/curl/CurlHandleContainer.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/http/standard/StandardHttpResponse.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>

#include <atomic>

using namespace Aws::Http;

// Curl implementation of an http client. Right now it is only synchronous.
class CustomCurlHttpClient : public HttpClient {
 public:
  using Base = HttpClient;

  // Creates client, initializes curl handle if it hasn't been created already.
  CustomCurlHttpClient(const Aws::Client::ClientConfiguration& clientConfig);

  // Makes request and receives response synchronously
  std::shared_ptr<HttpResponse> MakeRequest(
      const std::shared_ptr<HttpRequest>& request,
      Aws::Utils::RateLimits::RateLimiterInterface* readLimiter = nullptr,
      Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter =
          nullptr) const override;

  static void InitGlobalState();
  static void CleanupGlobalState();

 protected:
  /**
   * Override any configuration on CURL handle for each request before sending.
   * The usage is to have a subclass of CustomCurlHttpClient and have your own
   * implementation of this function to configure whatever you want on CURL
   * handle.
   */
  virtual void OverrideOptionsOnConnectionHandle(CURL*) const {}

 private:
  mutable CurlHandleContainer m_curlHandleContainer;
  bool m_isAllowSystemProxy = false;
  bool m_isUsingProxy = false;
  Aws::String m_proxyUserName;
  Aws::String m_proxyPassword;
  Aws::String m_proxyScheme;
  Aws::String m_proxyHost;
  Aws::String m_proxySSLCertPath;
  Aws::String m_proxySSLCertType;
  Aws::String m_proxySSLKeyPath;
  Aws::String m_proxySSLKeyType;
  Aws::String m_proxyKeyPasswd;
  unsigned m_proxyPort = 0;
  Aws::String m_nonProxyHosts;
  bool m_verifySSL = true;
  Aws::String m_caPath;
  Aws::String m_caFile;
  Aws::String m_proxyCaPath;
  Aws::String m_proxyCaFile;
  bool m_disableExpectHeader = false;
  bool m_allowRedirects = false;
  bool m_enableHttpClientTrace = false;
  Aws::Http::TransferLibPerformanceMode m_perfMode =
      TransferLibPerformanceMode::LOW_LATENCY;
  static std::atomic<bool> isInit;
  std::shared_ptr<smithy::components::tracing::TelemetryProvider>
      m_telemetryProvider;

#if LIBCURL_VERSION_NUM >= 0x072000  // 7.32.0
  static int CurlProgressCallback(void* userdata, curl_off_t, curl_off_t,
                                  curl_off_t, curl_off_t);
#else
  static int CurlProgressCallback(void* userdata, double, double, double,
                                  double);
#endif
};

class MyHttpClientFactory : public Aws::Http::HttpClientFactory {
 public:
  std::shared_ptr<Aws::Http::HttpClient> CreateHttpClient(
      const Aws::Client::ClientConfiguration& clientConfiguration)
      const override {
    return Aws::MakeShared<CustomCurlHttpClient>("MyAlloc",
                                                 clientConfiguration);
  }

  std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(
      const Aws::Http::URI& uri, Aws::Http::HttpMethod method,
      const Aws::IOStreamFactory& streamFactory) const override {
    auto request = Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>(
        "CustomCurlHttpClient", uri, method);
    request->SetResponseStreamFactory(streamFactory);
    return request;
  }

  std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(
      const Aws::String& uri, Aws::Http::HttpMethod method,
      const Aws::IOStreamFactory& streamFactory) const override {
    Aws::Http::URI parsedUri(uri);
    return CreateHttpRequest(parsedUri, method, streamFactory);
  }
};
