use anyhow::{Context as _, Result};
use cloud_llm_client::predict_edits_v3::{RawCompletionRequest, RawCompletionResponse};
use futures::AsyncReadExt as _;
use gpui::{App, AppContext as _, Entity, Global, SharedString, Task, http_client};
use language::language_settings::{
    EditPredictionSettings, OpenAiCompatibleEditPredictionSettings, all_language_settings,
};
use language_model::{ApiKeyState, EnvVar, env_var};
use std::{collections::HashMap, future::Future, sync::Arc};

pub const GROQ_API_URL: &str = "https://api.groq.com/openai/v1";
pub const CEREBRAS_API_URL: &str = "https://api.cerebras.ai/v1";

pub const OPEN_AI_COMPATIBLE_CREDENTIALS_USERNAME: &str = "openai-compatible-api-token";
pub static OPEN_AI_COMPATIBLE_TOKEN_ENV_VAR: std::sync::LazyLock<EnvVar> =
    env_var!("ZED_OPEN_AI_COMPATIBLE_EDIT_PREDICTION_API_KEY");

struct GlobalOpenAiCompatibleApiKeys(Entity<OpenAiCompatibleApiKeys>);

impl Global for GlobalOpenAiCompatibleApiKeys {}

struct OpenAiCompatibleApiKeys {
    keys_by_provider: HashMap<&'static str, Entity<ApiKeyState>>,
}

pub fn open_ai_compatible_api_url(cx: &App) -> SharedString {
    provider_api_url(settings::EditPredictionProvider::OpenAiCompatibleApi, cx)
}

pub fn groq_api_url(cx: &App) -> SharedString {
    provider_api_url(settings::EditPredictionProvider::Groq, cx)
}

pub fn cerebras_api_url(cx: &App) -> SharedString {
    provider_api_url(settings::EditPredictionProvider::Cerebras, cx)
}

pub fn open_ai_compatible_api_token(cx: &mut App) -> Entity<ApiKeyState> {
    provider_api_token(settings::EditPredictionProvider::OpenAiCompatibleApi, cx)
}

pub fn groq_api_token(cx: &mut App) -> Entity<ApiKeyState> {
    provider_api_token(settings::EditPredictionProvider::Groq, cx)
}

pub fn cerebras_api_token(cx: &mut App) -> Entity<ApiKeyState> {
    provider_api_token(settings::EditPredictionProvider::Cerebras, cx)
}

fn provider_api_token(
    provider: settings::EditPredictionProvider,
    cx: &mut App,
) -> Entity<ApiKeyState> {
    let registry = if let Some(global) = cx.try_global::<GlobalOpenAiCompatibleApiKeys>() {
        global.0.clone()
    } else {
        let registry = cx.new(|_cx| OpenAiCompatibleApiKeys {
            keys_by_provider: HashMap::new(),
        });
        cx.set_global(GlobalOpenAiCompatibleApiKeys(registry.clone()));
        registry
    };

    registry.update(cx, |registry, cx| {
        let provider_id = provider_id(provider);
        if let Some(entity) = registry.keys_by_provider.get(provider_id) {
            return entity.clone();
        }

        let entity = cx
            .new(|cx| ApiKeyState::new(provider_api_url(provider, cx), provider_env_var(provider)));
        registry
            .keys_by_provider
            .insert(provider_id, entity.clone());
        entity
    })
}

fn provider_id(provider: settings::EditPredictionProvider) -> &'static str {
    match provider {
        settings::EditPredictionProvider::OpenAiCompatibleApi => "open_ai_compatible_api",
        settings::EditPredictionProvider::Groq => "groq",
        settings::EditPredictionProvider::Cerebras => "cerebras",
        _ => "unknown",
    }
}

fn provider_display_name(provider: settings::EditPredictionProvider) -> &'static str {
    match provider {
        settings::EditPredictionProvider::OpenAiCompatibleApi => "OpenAI-Compatible API",
        settings::EditPredictionProvider::Groq => "Groq",
        settings::EditPredictionProvider::Cerebras => "Cerebras",
        _ => "Unknown",
    }
}

fn provider_env_var(provider: settings::EditPredictionProvider) -> EnvVar {
    match provider {
        settings::EditPredictionProvider::OpenAiCompatibleApi => {
            OPEN_AI_COMPATIBLE_TOKEN_ENV_VAR.clone()
        }
        settings::EditPredictionProvider::Groq => EnvVar::new("GROQ_API_KEY".into()),
        settings::EditPredictionProvider::Cerebras => EnvVar::new("CEREBRAS_API_KEY".into()),
        _ => EnvVar::new("".into()),
    }
}

fn provider_api_url(provider: settings::EditPredictionProvider, cx: &App) -> SharedString {
    let configured_url = configured_provider_api_url(provider, cx)
        .or_else(|| {
            provider_settings_for_global(provider, cx)
                .map(|settings| settings.api_url.as_ref().to_string())
        })
        .filter(|url| !url.is_empty());

    configured_url
        .unwrap_or_else(|| match provider {
            settings::EditPredictionProvider::Groq => GROQ_API_URL.into(),
            settings::EditPredictionProvider::Cerebras => CEREBRAS_API_URL.into(),
            _ => "".into(),
        })
        .into()
}

fn configured_provider_api_url(
    provider: settings::EditPredictionProvider,
    cx: &App,
) -> Option<String> {
    let edit_predictions = cx
        .try_global::<settings::SettingsStore>()?
        .merged_settings()
        .project
        .all_languages
        .edit_predictions
        .as_ref()?;

    raw_custom_settings_for_provider(edit_predictions, provider)
        .and_then(|settings| settings.api_url.clone())
}

fn raw_custom_settings_for_provider(
    settings: &settings::EditPredictionSettingsContent,
    provider: settings::EditPredictionProvider,
) -> Option<&settings::CustomEditPredictionProviderSettingsContent> {
    match provider {
        settings::EditPredictionProvider::OpenAiCompatibleApi => {
            settings.open_ai_compatible_api.as_ref()
        }
        settings::EditPredictionProvider::Groq => settings.groq.as_ref(),
        settings::EditPredictionProvider::Cerebras => settings.cerebras.as_ref(),
        _ => None,
    }
}

fn provider_settings_for_global(
    provider: settings::EditPredictionProvider,
    cx: &App,
) -> Option<&OpenAiCompatibleEditPredictionSettings> {
    custom_settings_for_provider(&all_language_settings(None, cx).edit_predictions, provider)
}

pub fn custom_settings_for_provider(
    settings: &EditPredictionSettings,
    provider: settings::EditPredictionProvider,
) -> Option<&OpenAiCompatibleEditPredictionSettings> {
    match provider {
        settings::EditPredictionProvider::Ollama => settings.ollama.as_ref(),
        settings::EditPredictionProvider::OpenAiCompatibleApi => {
            settings.open_ai_compatible_api.as_ref()
        }
        settings::EditPredictionProvider::Groq => settings.groq.as_ref(),
        settings::EditPredictionProvider::Cerebras => settings.cerebras.as_ref(),
        _ => None,
    }
}

pub fn load_open_ai_compatible_api_token(
    cx: &mut App,
) -> Task<Result<(), language_model::AuthenticateError>> {
    load_provider_api_token(settings::EditPredictionProvider::OpenAiCompatibleApi, cx)
}

pub fn load_provider_api_token(
    provider: settings::EditPredictionProvider,
    cx: &mut App,
) -> Task<Result<(), language_model::AuthenticateError>> {
    let credentials_provider = zed_credentials_provider::global(cx);
    let api_url = provider_api_url(provider, cx);
    provider_api_token(provider, cx).update(cx, |key_state, cx| {
        key_state.load_if_needed(api_url, |s| s, credentials_provider, cx)
    })
}

pub fn load_open_ai_compatible_api_keys_if_needed(
    provider: settings::EditPredictionProvider,
    cx: &mut App,
) -> Vec<Arc<str>> {
    if !matches!(
        provider,
        settings::EditPredictionProvider::OpenAiCompatibleApi
            | settings::EditPredictionProvider::Groq
            | settings::EditPredictionProvider::Cerebras
    ) {
        return Vec::new();
    }
    _ = load_provider_api_token(provider, cx);
    let url = provider_api_url(provider, cx);
    provider_api_token(provider, cx)
        .read(cx)
        .key(&url)
        .map(split_api_keys)
        .unwrap_or_default()
}

fn split_api_keys(api_key: Arc<str>) -> Vec<Arc<str>> {
    api_key
        .split([',', ';', '\n'])
        .map(str::trim)
        .filter(|key| !key.is_empty())
        .map(Arc::from)
        .collect()
}

pub(crate) async fn send_custom_server_request(
    provider: settings::EditPredictionProvider,
    settings: &OpenAiCompatibleEditPredictionSettings,
    prompt: String,
    max_tokens: u32,
    stop_tokens: Vec<String>,
    api_keys: Vec<Arc<str>>,
    http_client: &Arc<dyn http_client::HttpClient>,
) -> Result<(String, String)> {
    match provider {
        settings::EditPredictionProvider::Ollama => {
            let response = crate::ollama::make_request(
                settings.clone(),
                prompt,
                stop_tokens,
                http_client.clone(),
            )
            .await?;
            Ok((response.response, response.created_at))
        }
        settings::EditPredictionProvider::Groq | settings::EditPredictionProvider::Cerebras => {
            if api_keys.is_empty() {
                anyhow::bail!("missing API key for {}", provider_display_name(provider));
            }

            send_with_api_key_fallback(provider, &api_keys, |api_key| async {
                send_open_ai_chat_completion_request(
                    settings,
                    prompt.clone(),
                    max_tokens,
                    stop_tokens.clone(),
                    api_key,
                    http_client,
                )
                .await
            })
            .await
        }
        _ => {
            let request = RawCompletionRequest {
                model: settings.model.clone(),
                prompt,
                max_tokens: Some(max_tokens),
                temperature: None,
                stop: stop_tokens
                    .into_iter()
                    .map(std::borrow::Cow::Owned)
                    .collect(),
                environment: None,
            };

            let request_body = serde_json::to_string(&request)?;
            if api_keys.is_empty() {
                send_raw_completion_request(settings, &request_body, None, http_client).await
            } else {
                send_with_api_key_fallback(provider, &api_keys, |api_key| async {
                    send_raw_completion_request(settings, &request_body, Some(api_key), http_client)
                        .await
                })
                .await
            }
        }
    }
}

async fn send_with_api_key_fallback<'a, F, Fut>(
    provider: settings::EditPredictionProvider,
    api_keys: &'a [Arc<str>],
    mut send: F,
) -> Result<(String, String)>
where
    F: FnMut(&'a str) -> Fut,
    Fut: Future<Output = Result<(String, String)>>,
{
    let mut last_error = None;
    let key_count = api_keys.len();

    for (key_index, api_key) in api_keys.iter().enumerate() {
        match send(api_key.as_ref()).await {
            Ok(response) => return Ok(response),
            Err(error) => {
                log::warn!(
                    "{} edit prediction request failed with API key {}/{}",
                    provider_display_name(provider),
                    key_index + 1,
                    key_count
                );
                last_error = Some(error);
            }
        }
    }

    if let Some(error) = last_error {
        Err(error).with_context(|| {
            format!(
                "{} edit prediction request failed with all configured API keys",
                provider_display_name(provider)
            )
        })
    } else {
        anyhow::bail!("missing API key for {}", provider_display_name(provider));
    }
}

async fn send_open_ai_chat_completion_request(
    settings: &OpenAiCompatibleEditPredictionSettings,
    prompt: String,
    max_tokens: u32,
    stop_tokens: Vec<String>,
    api_key: &str,
    http_client: &Arc<dyn http_client::HttpClient>,
) -> Result<(String, String)> {
    let request = open_ai::Request {
        model: settings.model.clone(),
        messages: vec![open_ai::RequestMessage::User {
            content: open_ai::MessageContent::Plain(prompt),
        }],
        stream: false,
        stream_options: None,
        max_completion_tokens: Some(max_tokens.into()),
        max_tokens: None,
        stop: stop_tokens,
        temperature: None,
        tool_choice: None,
        parallel_tool_calls: None,
        tools: Vec::new(),
        prompt_cache_key: None,
        reasoning_effort: None,
        service_tier: None,
    };

    let response = open_ai::non_streaming_completion(
        http_client.as_ref(),
        settings.api_url.as_ref(),
        api_key,
        request,
    )
    .await?;

    let text = response
        .choices
        .into_iter()
        .next()
        .and_then(|choice| match choice.message {
            open_ai::RequestMessage::Assistant {
                content: Some(open_ai::MessageContent::Plain(text)),
                ..
            } => Some(text),
            open_ai::RequestMessage::Assistant {
                content: Some(open_ai::MessageContent::Multipart(parts)),
                ..
            } => Some(
                parts
                    .into_iter()
                    .filter_map(|part| match part {
                        open_ai::MessagePart::Text { text } => Some(text),
                        _ => None,
                    })
                    .collect(),
            ),
            _ => None,
        })
        .unwrap_or_default();

    Ok((text, response.id))
}

async fn send_raw_completion_request(
    settings: &OpenAiCompatibleEditPredictionSettings,
    request_body: &str,
    api_key: Option<&str>,
    http_client: &Arc<dyn http_client::HttpClient>,
) -> Result<(String, String)> {
    let mut http_request_builder = http_client::Request::builder()
        .method(http_client::Method::POST)
        .uri(settings.api_url.as_ref())
        .header("Content-Type", "application/json");

    if let Some(api_key) = api_key {
        http_request_builder =
            http_request_builder.header("Authorization", format!("Bearer {}", api_key));
    }

    let http_request =
        http_request_builder.body(http_client::AsyncBody::from(request_body.to_owned()))?;

    let mut response = http_client.send(http_request).await?;
    let status = response.status();

    if !status.is_success() {
        let mut body = String::new();
        response.body_mut().read_to_string(&mut body).await?;
        anyhow::bail!("custom server error: {} - {}", status, body);
    }

    let mut body = String::new();
    response.body_mut().read_to_string(&mut body).await?;

    let parsed: RawCompletionResponse =
        serde_json::from_str(&body).context("Failed to parse completion response")?;
    let text = parsed
        .choices
        .into_iter()
        .next()
        .map(|choice| choice.text)
        .unwrap_or_default();
    Ok((text, parsed.id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::{
        BorrowAppContext, TestAppContext,
        http_client::{AsyncBody, FakeHttpClient, Method, Response, StatusCode},
    };
    use parking_lot::Mutex;
    use serde_json::{Value, json};
    use settings::SettingsStore;
    use std::sync::Arc;

    #[derive(Debug)]
    struct CapturedRequest {
        method: Method,
        uri: String,
        authorization: Option<String>,
        body: Value,
    }

    #[test]
    fn test_split_api_keys() {
        let keys = split_api_keys(" key-1, key-2;key-3\n\nkey-4 ".into());

        assert_eq!(
            keys.iter().map(|key| key.as_ref()).collect::<Vec<_>>(),
            vec!["key-1", "key-2", "key-3", "key-4"]
        );
    }

    #[gpui::test]
    fn test_provider_api_url_uses_partial_custom_url_before_model(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let settings_store = SettingsStore::test(cx);
            cx.set_global(settings_store);

            cx.update_global::<SettingsStore, _>(|store, cx| {
                store.update_user_settings(cx, |settings| {
                    settings.project.all_languages.edit_predictions =
                        Some(settings::EditPredictionSettingsContent {
                            groq: Some(settings::CustomEditPredictionProviderSettingsContent {
                                api_url: Some("https://proxy.example.test/openai/v1".into()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        });
                });
            });

            assert!(
                all_language_settings(None, cx)
                    .edit_predictions
                    .groq
                    .is_none()
            );
            assert_eq!(
                groq_api_url(cx).as_str(),
                "https://proxy.example.test/openai/v1"
            );
        });
    }

    #[gpui::test]
    fn test_groq_model_only_uses_default_provider_settings(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let settings_store = SettingsStore::test(cx);
            cx.set_global(settings_store);

            cx.update_global::<SettingsStore, _>(|store, cx| {
                store.update_user_settings(cx, |settings| {
                    settings.project.all_languages.edit_predictions =
                        Some(settings::EditPredictionSettingsContent {
                            provider: Some(settings::EditPredictionProvider::Groq),
                            groq: Some(settings::CustomEditPredictionProviderSettingsContent {
                                model: Some("qwen/qwen3-32b".into()),
                                ..Default::default()
                            }),
                            ..Default::default()
                        });
                });
            });

            let groq_settings = all_language_settings(None, cx)
                .edit_predictions
                .groq
                .as_ref()
                .expect("Groq settings should be configured by model plus defaults");
            assert_eq!(groq_settings.api_url.as_ref(), GROQ_API_URL);
            assert_eq!(groq_settings.model, "qwen/qwen3-32b");
            assert_eq!(groq_settings.max_output_tokens, 64);
            assert_eq!(
                groq_settings.prompt_format,
                settings::EditPredictionPromptFormat::Infer
            );
            assert_eq!(groq_api_url(cx).as_str(), GROQ_API_URL);
        });
    }

    #[test]
    fn test_groq_chat_completion_request_shape() {
        gpui::block_on(assert_chat_completion_request_shape(
            settings::EditPredictionProvider::Groq,
            GROQ_API_URL,
            "groq-model",
        ));
    }

    #[test]
    fn test_cerebras_chat_completion_request_shape() {
        gpui::block_on(assert_chat_completion_request_shape(
            settings::EditPredictionProvider::Cerebras,
            CEREBRAS_API_URL,
            "cerebras-model",
        ));
    }

    #[test]
    fn test_open_ai_compatible_raw_completion_still_uses_raw_endpoint() {
        gpui::block_on(async {
            let captured_requests = Arc::new(Mutex::new(Vec::new()));
            let http_client = fake_http_client(captured_requests.clone(), |_| {
                response(
                    StatusCode::OK,
                    json!({
                        "id": "raw-response-id",
                        "object": "text_completion",
                        "created": 1,
                        "model": "raw-model",
                        "choices": [{
                            "text": "raw completion text",
                            "finish_reason": "stop"
                        }],
                        "usage": {
                            "prompt_tokens": 1,
                            "completion_tokens": 2,
                            "total_tokens": 3
                        }
                    }),
                )
            });

            let (text, response_id) = send_custom_server_request(
                settings::EditPredictionProvider::OpenAiCompatibleApi,
                &test_settings("https://example.test/completions", "raw-model"),
                "raw prompt".into(),
                17,
                vec!["</fim>".into()],
                vec!["raw-key".into()],
                &http_client,
            )
            .await
            .expect("raw completion request succeeds");

            assert_eq!(text, "raw completion text");
            assert_eq!(response_id, "raw-response-id");

            let requests = captured_requests.lock();
            assert_eq!(requests.len(), 1);
            let request = &requests[0];
            assert_eq!(request.method, Method::POST);
            assert_eq!(request.uri, "https://example.test/completions");
            assert!(!request.uri.ends_with("/chat/completions"));
            assert_eq!(request.authorization.as_deref(), Some("Bearer raw-key"));
            assert_eq!(request.body["model"], "raw-model");
            assert_eq!(request.body["prompt"], "raw prompt");
            assert_eq!(request.body["max_tokens"], 17);
            assert_eq!(request.body["stop"], json!(["</fim>"]));
        });
    }

    #[test]
    fn test_api_key_fallback_retries_second_key_after_failure() {
        gpui::block_on(async {
            let captured_requests = Arc::new(Mutex::new(Vec::new()));
            let http_client = fake_http_client(captured_requests.clone(), |request_index| {
                if request_index == 0 {
                    response(
                        StatusCode::UNAUTHORIZED,
                        json!({
                            "error": {
                                "message": "bad key"
                            }
                        }),
                    )
                } else {
                    response(
                        StatusCode::OK,
                        chat_completion_response("fallback-response-id", "second key text"),
                    )
                }
            });

            let (text, response_id) = send_custom_server_request(
                settings::EditPredictionProvider::Groq,
                &test_settings(GROQ_API_URL, "groq-model"),
                "prompt".into(),
                8,
                Vec::new(),
                vec!["first-key".into(), "second-key".into()],
                &http_client,
            )
            .await
            .expect("second API key succeeds");

            assert_eq!(text, "second key text");
            assert_eq!(response_id, "fallback-response-id");

            let requests = captured_requests.lock();
            assert_eq!(requests.len(), 2);
            assert_eq!(
                requests[0].authorization.as_deref(),
                Some("Bearer first-key")
            );
            assert_eq!(
                requests[1].authorization.as_deref(),
                Some("Bearer second-key")
            );
        });
    }

    #[test]
    fn test_api_key_fallback_reports_all_keys_failed() {
        gpui::block_on(async {
            let captured_requests = Arc::new(Mutex::new(Vec::new()));
            let http_client = fake_http_client(captured_requests.clone(), |_| {
                response(
                    StatusCode::UNAUTHORIZED,
                    json!({
                        "error": {
                            "message": "bad key"
                        }
                    }),
                )
            });

            let error = send_custom_server_request(
                settings::EditPredictionProvider::Cerebras,
                &test_settings(CEREBRAS_API_URL, "cerebras-model"),
                "prompt".into(),
                8,
                Vec::new(),
                vec!["first-key".into(), "second-key".into()],
                &http_client,
            )
            .await
            .expect_err("all API keys fail");

            assert!(
                format!("{error:#}").contains(
                    "Cerebras edit prediction request failed with all configured API keys"
                )
            );

            let requests = captured_requests.lock();
            assert_eq!(requests.len(), 2);
            assert_eq!(
                requests[0].authorization.as_deref(),
                Some("Bearer first-key")
            );
            assert_eq!(
                requests[1].authorization.as_deref(),
                Some("Bearer second-key")
            );
        });
    }

    async fn assert_chat_completion_request_shape(
        provider: settings::EditPredictionProvider,
        api_url: &str,
        model: &str,
    ) {
        let captured_requests = Arc::new(Mutex::new(Vec::new()));
        let http_client = fake_http_client(captured_requests.clone(), |_| {
            response(
                StatusCode::OK,
                chat_completion_response("chat-response-id", "chat completion text"),
            )
        });

        let (text, response_id) = send_custom_server_request(
            provider,
            &test_settings(api_url, model),
            "fim prompt".into(),
            19,
            vec!["<stop-1>".into(), "<stop-2>".into()],
            vec!["chat-key".into()],
            &http_client,
        )
        .await
        .expect("chat completion request succeeds");

        assert_eq!(text, "chat completion text");
        assert_eq!(response_id, "chat-response-id");

        let requests = captured_requests.lock();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(request.method, Method::POST);
        assert_eq!(request.uri, format!("{api_url}/chat/completions"));
        assert_eq!(request.authorization.as_deref(), Some("Bearer chat-key"));
        assert_eq!(request.body["model"], model);
        assert_eq!(request.body["messages"][0]["role"], "user");
        assert_eq!(request.body["messages"][0]["content"], "fim prompt");
        assert_eq!(request.body["max_completion_tokens"], 19);
        assert_eq!(request.body["stop"], json!(["<stop-1>", "<stop-2>"]));
    }

    fn test_settings(api_url: &str, model: &str) -> OpenAiCompatibleEditPredictionSettings {
        OpenAiCompatibleEditPredictionSettings {
            model: model.into(),
            max_output_tokens: 64,
            api_url: api_url.into(),
            prompt_format: settings::EditPredictionPromptFormat::Qwen,
        }
    }

    fn fake_http_client(
        captured_requests: Arc<Mutex<Vec<CapturedRequest>>>,
        response_for_request: impl Fn(usize) -> Response<AsyncBody> + Send + Sync + 'static,
    ) -> Arc<dyn http_client::HttpClient> {
        FakeHttpClient::create(move |mut request| {
            let captured_requests = captured_requests.clone();
            let response = response_for_request(captured_requests.lock().len());
            async move {
                let method = request.method().clone();
                let uri = request.uri().to_string();
                let authorization = request
                    .headers()
                    .get("Authorization")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned);
                let mut body = String::new();
                request.body_mut().read_to_string(&mut body).await?;
                let body = serde_json::from_str(&body)?;

                captured_requests.lock().push(CapturedRequest {
                    method,
                    uri,
                    authorization,
                    body,
                });

                Ok(response)
            }
        })
    }

    fn response(status: StatusCode, body: Value) -> Response<AsyncBody> {
        Response::builder()
            .status(status)
            .body(AsyncBody::from(body.to_string()))
            .expect("build fake response")
    }

    fn chat_completion_response(id: &str, content: &str) -> Value {
        json!({
            "id": id,
            "object": "chat.completion",
            "created": 1,
            "model": "chat-model",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": content
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 2,
                "total_tokens": 3
            }
        })
    }
}
