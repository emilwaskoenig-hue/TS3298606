# Power to the People? Retail Investors, ESG Proxy Voting, and the Say-Do Gap in U.S. Shareholder Proposals, 2020–2025

Replication and data repository for the master's thesis of the same title.

**Author:** Emil Waskönig
**Advisor:** Professor Paolo Volpin
**Programme:** MSc in Transformative Sustainability, a joint degree of Università Bocconi and Politecnico di Milano

## About

This repository holds the full classification pipeline, intermediate outputs, and final dataset behind the thesis. The thesis tests whether retail investors translate their stated environmental and social (ES) preferences into proxy votes, and whether that behaviour shifted following the 2025 reversal in U.S. ESG policy. Proposal-level ESG orientation is constructed from SEC EDGAR filings through a two-agent large language model pipeline: Agent 1 (GPT-4o-mini) extracts proposal text, and Agent 2 (Claude Sonnet) scores each proposal on ESG relevance, direction, and specificity across the E, S, and G dimensions. The files below let any reader reproduce and audit every step from raw filing to final regression input.

## Files

| File | Contents |
|------|----------|
| `APPENDIX_A_main_run.py` | The full two-agent classification pipeline: EDGAR retrieval, Agent 1 text extraction, Agent 2 scoring, and output writing. |
| `APPENDIX_B_Two-Shot Examples.xlsx` | The two-shot examples used to anchor Agent 1's text-extraction format. |
| `APPENDIX_C_Manual Examples.xlsx` | The manually annotated proposals used as Agent 2's few-shot examples and as the held-out validation set. |
| `APPENDIX_D_main_run_output.csv` | Raw output of the main classification run, one row per proposal with all scores, confidence ratings, and model reasoning. |
| `APPENDIX_E_Manual_Review.csv` | The low-confidence and boundary-case queue flagged for manual review. |
| `APPENDIX_F_Manual_Review_rescored.csv` | The manual-review cases after re-extraction and rescoring. |
| `APPENDIX_G_final_dataset.csv` / `.gsheet` | The final analytical dataset: cleaned classification output matched to LSEG institutional ownership and the constructed retail ownership variable. |
| `APPENDIX_H_spotcheck_rerun_results.csv` | The 88-proposal spot-check rerun used to assess inter-run agreement and validate classification stability. |

## Note

ESG scoring uses Claude Sonnet at `temperature = 0` for deterministic, reproducible output. Running `APPENDIX_A_main_run.py` requires valid OpenAI and Anthropic API keys.
