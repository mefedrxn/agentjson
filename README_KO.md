# agentjson

Rust 기반의 확률적 JSON 복구 라이브러리 (Python 바인딩 포함). 표준 파서가 거부하는 심각하게 깨진 JSON을 처리합니다.

## 특징

- **Fast Path**: 정상 JSON은 즉시 파싱
- **휴리스틱 복구**: Beam Search 전에 자동 수정 적용
- **확률적 Beam Search**: 신뢰도 점수와 함께 Top-K 복구 후보 반환
- **LLM 폴백**: 극단적인 케이스를 위한 선택적 LLM 지원 복구

### 수정 가능한 문제들

| 문제 유형 | 예시 | 수정 결과 |
|----------|------|----------|
| 따옴표 없는 키 | `{name: "Alice"}` | `{"name": "Alice"}` |
| 작은따옴표 | `{'key': 'value'}` | `{"key": "value"}` |
| Python 리터럴 | `{"a": True, "b": None}` | `{"a": true, "b": null}` |
| 후행 콤마 | `{"a": 1, "b": 2,}` | `{"a": 1, "b": 2}` |
| 누락된 콤마 | `{"a": 1 "b": 2}` | `{"a": 1, "b": 2}` |
| JS 주석 | `{/* comment */ "a": 1}` | `{"a": 1}` |
| 따옴표 없는 배열 값 | `[admin, user]` | `["admin", "user"]` |
| Markdown 코드 펜스 | `` ```json {...} ``` `` | `{...}` |
| 앞뒤 불필요한 텍스트 | `Response: {...} EOF` | `{...}` |
| 닫히지 않은 문자열/괄호 | `{"a": "hello` | `{"a": "hello"}` |

## 설치

### 1. Rust 툴체인 설치

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### 2. PyO3 확장과 함께 패키지 설치

```bash
# 저장소 클론
git clone https://github.com/sionic-ai/json-prob-parser.git
cd json-prob-parser

# 가상 환경 생성
python -m venv .venv
source .venv/bin/activate  # Windows: `.venv\Scripts\activate`

# maturin 설치 및 빌드
pip install maturin
maturin develop

# Python 패키지 설치
pip install -e .
```

## 빠른 시작

### Python 라이브러리

```python
from agentjson import parse, RepairOptions

# 간단한 사용법
result = parse('{"a": 1, "b": 2,}')  # 후행 콤마
print(result.status)           # "repaired"
print(result.best.value)       # {'a': 1, 'b': 2}

# 옵션과 함께 사용
result = parse(
    '''```json
    {
        name: "Alice",
        age: 30,
        active: True,
        roles: [admin, user,]
    }
    ```''',
    RepairOptions(
        mode="auto",
        top_k=3,
        beam_width=32,
        max_repairs=50,
    ),
)

print(result.status)                    # "repaired"
print(result.best.value)                # {'name': 'Alice', 'age': 30, ...}
print(len(result.best.repairs))         # 적용된 복구 횟수
print(result.metrics.elapsed_ms)        # 처리 시간
```

### CLI

```bash
# stdin에서 입력
echo '{"a": 1, "b": 2,}' | agentjson

# 파일에서 입력
agentjson --input broken.json

# 옵션과 함께 사용
agentjson --input broken.json \
    --mode probabilistic \
    --beam-width 64 \
    --max-repairs 100 \
    --top-k 5
```

### CLI 옵션

| 옵션 | 기본값 | 설명 |
|------|-------|------|
| `--input`, `-i` | stdin | 입력 파일 경로 |
| `--mode` | `auto` | `auto`, `strict_only`, `fast_repair`, `probabilistic`, `scale_pipeline` |
| `--scale-output` | `dom` | `dom` (JSON 구체화) 또는 `tape` (IR만 반환; value는 null) |
| `--top-k` | 5 | 반환할 복구 후보 수 |
| `--beam-width` | 32 | Beam Search 너비 |
| `--max-repairs` | 20 | 후보당 최대 복구 연산 수 |
| `--partial-ok` | true | 실패 시 부분 결과 허용 |
| `--allow-llm` | false | 극단적 케이스를 위한 LLM 폴백 활성화 |
| `--llm-provider` | `none` | `none`, `anthropic`, `claude_agent_sdk` |
| `--llm-mode` | `patch_suggest` | `patch_suggest` 또는 `token_suggest` (patch 권장) |
| `--llm-min-confidence` | 0.2 | 이 신뢰도 이하일 때 LLM 트리거 |
| `--debug` | false | 디버그 정보 포함 |

## 복구 파이프라인

```
입력 텍스트
    │
    ▼
┌─────────────────┐
│ 1. 추출         │  Markdown 펜스, 앞뒤 불필요 텍스트 제거
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 2. 휴리스틱     │  빠른 수정: 따옴표, 주석, 리터럴, 콤마
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 3. Strict 파싱  │  표준 JSON 파싱 시도
└────────┬────────┘
         │ (실패 시)
         ▼
┌─────────────────┐
│ 4. Beam Search  │  Top-K 후보와 함께 확률적 복구
└────────┬────────┘
         │ (낮은 신뢰도 시)
         ▼
┌─────────────────┐
│ 5. LLM 폴백     │  선택사항: Claude 지원 복구
└────────┬────────┘
         │
         ▼
    RepairResult
```

## LLM 심층 복구 (선택사항)

Beam Search의 신뢰도가 낮은 심각하게 손상된 JSON의 경우 LLM 지원 복구를 활성화할 수 있습니다.

### 옵션 A) Anthropic SDK (간단)

```bash
python -m pip install anthropic
export ANTHROPIC_API_KEY=...
export CLAUDE_MODEL=claude-3-5-sonnet-latest
```

```python
from json_prob_parser import AnthropicPatchSuggestProvider, RepairOptions, parse

result = parse(
    '{"a":1,"b":2, completely broken garbage here',
    RepairOptions(
        mode="probabilistic",
        allow_llm=True,
        llm_mode="patch_suggest",
        llm_min_confidence=0.2,
        llm_provider=AnthropicPatchSuggestProvider(),
    ),
)

print(result.metrics.llm_calls)    # LLM 호출 횟수
print(result.metrics.llm_time_ms)  # LLM 처리 시간
```

### 옵션 B) Claude Agent SDK (도구: 메모리, 웹 검색, 스킬 등)

```python
from json_prob_parser import RepairOptions, parse
from json_prob_parser.claude_agent_sdk_provider import ClaudeAgentSDKProvider

# Claude Agent SDK 에이전트 설정
agent = ...  # 에이전트 인스턴스
provider = ClaudeAgentSDKProvider(agent=agent)

result = parse(
    '{"a":1,"b":2, completely broken garbage here',
    RepairOptions(
        mode="probabilistic",
        allow_llm=True,
        llm_mode="patch_suggest",
        llm_min_confidence=0.2,
        llm_provider=provider,
    ),
)

print(result.metrics.llm_calls)     # LLM 호출 횟수
print(result.metrics.llm_time_ms)   # LLM 처리 시간
```

## 결과 구조

```python
result = parse(text, options)

result.status          # "strict_ok" | "repaired" | "partial" | "failed"
result.best            # 최선의 후보 (candidates[best_index]의 단축)
result.best_index      # 최선의 후보 인덱스
result.candidates      # 복구 후보 목록

# 각 후보의 속성:
candidate.value           # 파싱된 Python 객체
candidate.normalized_json # 정규화된 JSON 문자열
candidate.confidence      # 신뢰도 점수 (0-1)
candidate.cost           # 총 복구 비용
candidate.repairs        # 적용된 복구 연산 목록

# 각 복구 연산:
repair.op        # 연산 이름 (예: "wrap_unquoted_key")
repair.span      # (start, end) 바이트 위치
repair.cost_delta # 이 복구의 비용
repair.note      # 사람이 읽을 수 있는 설명
```

## 대용량 처리 (Scale Pipeline)

GB 단위의 대용량 JSON 파일을 병렬로 처리할 수 있습니다.

### ROOT_ARRAY_ELEMENTS 모드

루트가 배열인 경우 `[elem0, elem1, ...]` 원소 단위로 병렬 분할:

```python
result = parse(
    huge_array_json,
    RepairOptions(
        mode="scale_pipeline",
        allow_parallel=True,
        parallel_workers=4,
    ),
)
```

### ROOT_OBJECT_PAIRS 모드

루트가 객체인 경우 `{"k0": v0, "k1": v1, ...}` KV 쌍 단위로 병렬 분할:

```python
result = parse(
    huge_object_json,
    RepairOptions(
        mode="scale_pipeline",
        allow_parallel=True,
    ),
)
```

### Tape/IR 모드 (Zero-Copy)

메모리 효율을 위해 DOM 대신 오프셋 기반 IR 반환:

```python
result = parse(
    huge_json,
    RepairOptions(
        mode="scale_pipeline",
        scale_output="tape",  # DOM 대신 Tape IR 반환
    ),
)

# result.best.value는 None
# result.best.ir에 Tape 구조 포함
print(result.best.ir['tape']['entry_count'])
```

`tape`는 대용량 처리를 위한 내부 **IR(중간 표현)** 입니다:

- JSON을 “토큰 스트림(평면 배열)” 형태의 `TapeEntry`들로 저장합니다(토큰 타입 + 원본 입력의 byte `offset`/`length`).
- 컨테이너(`array_start`/`object_start`)는 payload로 “매칭되는 end 엔트리”로 점프할 수 있는 인덱스를 가집니다.
- DOM을 만들지 않아도 구조를 추적/병렬 merge할 수 있어서 `scale_pipeline`에서 유리합니다.

## 개발

### 테스트 실행

```bash
# Rust 테스트
cd rust && cargo test

# Python 테스트 (PyO3가 설치되지 않으면 파싱 테스트 스킵)
PYTHONPATH=src python -m unittest discover -s tests -p 'test*.py' -v
```

### Rust CLI 빌드 (단독 실행)

```bash
cd rust
cargo build --release
./target/release/agentjson --input ../demo/broken.json
```

## 아키텍처

```
agentjson/
├── rust/                    # 핵심 Rust 라이브러리
│   └── src/
│       ├── heuristic.rs     # 휴리스틱 복구
│       ├── beam.rs          # Beam Search 알고리즘
│       ├── pipeline.rs      # 파싱 파이프라인 오케스트레이션
│       ├── tape.rs          # Tape/IR zero-copy 구조
│       ├── scale.rs         # 대용량 병렬 처리
│       └── ...
├── rust-pyo3/               # PyO3 Python 바인딩
│   └── src/lib.rs
└── src/json_prob_parser/    # Python 패키지
    ├── pipeline.py          # Python 파이프라인 (Rust + 선택적 LLM)
    ├── rust_core.py         # 얇은 PyO3 브릿지
    ├── anthropic_provider.py
    ├── claude_agent_sdk_provider.py
    ├── llm.py               # LLM 페이로드 + 패치 연산
    └── types.py             # 데이터 클래스
```

## 설계 철학

이 프로젝트는 다음 원칙에 따라 설계되었습니다:

1. **정상 입력은 빠르게 끝낸다** - Fast Path 최우선
2. **깨진 입력은 멈추지 않는다** - 내결함성
3. **불확실성은 Top-K로 드러낸다** - 확률적 파서
4. **대용량은 IR 우선** - DOM/GC 병목 회피
5. **병렬화는 조건부 + 안전 경계 기반** - 오버헤드 최소화
6. **모델 개입은 국소적·제한적** - 비용/재현성 관리

## 라이선스

MIT OR Apache-2.0
