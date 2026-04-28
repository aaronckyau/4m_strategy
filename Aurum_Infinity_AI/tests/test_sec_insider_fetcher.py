from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def load_sec_fetcher():
    module_name = "sec_insider_fetcher_test"
    module_path = Path(__file__).resolve().parents[2] / "Aurum_Data_Fetcher" / "fetch_sec_insider.py"
    data_fetcher_dir = str(module_path.parent)
    isolated_module_names = ["utils"]
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None

    original_sys_path = list(sys.path)
    original_modules = {
        name: sys.modules.get(name)
        for name in isolated_module_names
        if name in sys.modules
    }
    try:
        if data_fetcher_dir not in sys.path:
            sys.path.insert(0, data_fetcher_dir)
        for name in isolated_module_names:
            sys.modules.pop(name, None)
        spec.loader.exec_module(module)
    finally:
        sys.path[:] = original_sys_path
        for name in isolated_module_names:
            sys.modules.pop(name, None)
        sys.modules.update(original_modules)
    return module


def test_parse_master_index_filters_form_4_rows():
    fetcher = load_sec_fetcher()
    text = "\n".join([
        "header",
        "CIK|Company Name|Form Type|Date Filed|File Name",
        "0001|Apple Inc.|4|2026-04-20|edgar/data/1/0001-26-000001.txt",
        "0002|Ignored Co|10-K|2026-04-20|edgar/data/2/0002-26-000001.txt",
        "0003|Microsoft Corp.|4/A|2026-04-20|edgar/data/3/0003-26-000001.txt",
    ])

    rows = fetcher.parse_master_index(text)

    assert [row["form_type"] for row in rows] == ["4", "4/A"]
    assert [row["company_name"] for row in rows] == ["Apple Inc.", "Microsoft Corp."]


def test_parse_filing_extracts_non_derivative_purchase():
    fetcher = load_sec_fetcher()
    xml_text = """
    <ownershipDocument>
      <documentType>4</documentType>
      <periodOfReport>2026-04-20</periodOfReport>
      <issuer>
        <issuerCik>0000320193</issuerCik>
        <issuerName>Apple Inc.</issuerName>
        <issuerTradingSymbol>AAPL</issuerTradingSymbol>
      </issuer>
      <reportingOwner>
        <reportingOwnerId>
          <rptOwnerCik>0000001</rptOwnerCik>
          <rptOwnerName>CEO One</rptOwnerName>
        </reportingOwnerId>
        <reportingOwnerRelationship>
          <isDirector>1</isDirector>
          <isOfficer>1</isOfficer>
          <officerTitle>CEO</officerTitle>
        </reportingOwnerRelationship>
      </reportingOwner>
      <nonDerivativeTable>
        <nonDerivativeTransaction>
          <transactionDate><value>2026-04-19</value></transactionDate>
          <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
          <transactionAmounts>
            <transactionShares><value>100</value></transactionShares>
            <transactionPricePerShare><value>12.5</value></transactionPricePerShare>
            <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
          </transactionAmounts>
          <postTransactionAmounts>
            <sharesOwnedFollowingTransaction><value>1000</value></sharesOwnedFollowingTransaction>
          </postTransactionAmounts>
        </nonDerivativeTransaction>
      </nonDerivativeTable>
    </ownershipDocument>
    """

    filing, trades = fetcher.parse_filing(
        xml_text=xml_text,
        xml_url="https://sec.example/doc.xml",
        index_url="https://sec.example/index.html",
        accession_no="0000320193-26-000001",
        filing_date="20260420",
    )

    assert filing["issuer_symbol"] == "AAPL"
    assert filing["filing_date"] == "2026-04-20"
    assert len(trades) == 1
    assert trades[0]["transaction_type"] == "P-Purchase"
    assert trades[0]["securities_transacted_value"] == 1250
    assert "CEO" in trades[0]["type_of_owner"]
