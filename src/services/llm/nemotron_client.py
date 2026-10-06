import os, requests

NEMOTRON_ENDPOINT = "https://integrate.api.nvidia.com/v1/chat/completions"
NEMOTRON_MODEL = "nvidia/llama-3.1-nemotron-ultra-253b-v1"   

def _call(prompt: str, max_tokens: int = 300) -> str:
    resp = requests.post(
        NEMOTRON_ENDPOINT,
        headers={"Authorization": f"Bearer {os.environ['NVIDIA_API_KEY']}"},
        json={"model": NEMOTRON_MODEL, "messages": [{"role": "user", "content": prompt}],
              "max_tokens": max_tokens, "temperature": 0.2},
        timeout=20,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]

def suggest_prior_ranges(trigger_event: dict, historical_genomes: list[dict]) -> dict:
    prompt = (f"Trigger: {trigger_event['trigger_category']} severity {trigger_event['severity']}.\n"
              f"Past successful genomes for similar events: {historical_genomes}\n"
              "Suggest a narrow numeric search range for cpu_cores and replica_count "
              "as JSON: {\"cpu_cores\": [min,max], \"replica_count\": [min,max]}")
    return _call(prompt)  

def explain_promotion(winner_scores: dict, runner_up_scores: dict) -> str:
    prompt = (f"Winning config domain scores: {winner_scores}\n"
              f"Runner-up domain scores: {runner_up_scores}\n"
              "In one sentence, explain why the winner was chosen, for an ops dashboard.")
    return _call(prompt, max_tokens=60)