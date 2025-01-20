from .nfts_parser import NFTItemsParser
from pytoniq_core import Cell

parser = NFTItemsParser(None)

def test_offchain_metadata():
    metadata = parser.parse_metadata(Cell.one_from_boc("te6ccgEBAgEARwABbgFodHRwczovL3MuZ2V0Z2Vtcy5pby9uZnQvYi9jLzYyZTU1OGQ3NDRmMGMyMWY4MmJjNjU4ZC8BABYzL21ldGEuanNvbg=="))
    assert metadata == {'uri': 'https://s.getgems.io/nft/b/c/62e558d744f0c21f82bc658d/3/meta.json'}

def test_onchain_metadata_direct():
    # values stored directly in the cell
    metadata = parser.parse_metadata(Cell.one_from_boc("te6ccgECEAEAAp8AAQMAwAECASACBAFDv/CC62Y7V6ABkvSmrEZyiN8t/t252hvuKPZSHIvr0h8ewAMAVgBodHRwczovL3RvbmNvLmlvL3N0YXRpYy90b25jby1sb2dvLW5mdC5wbmcCASAFBwFCv4KjU3/w285+7DXWntw6GJ7m8X2C81OlU/mqlssL486JBgA2AFBvb2wgd1RUb24tVVNE4oKuIFBvc2l0aW9uAgEgCA4BQ79SCN70b1odT53OZqswn0qFEwXxZvke952SPvWONPmiCAEJAU4AIzM4IExQIFBvc2l0aW9uOiBbIC04ODcyMjAgLT4gODg3MjIwIF0KAQALAfYKVGhpcyBORlQgcmVwcmVzZW50cyBhIGxpcXVpZGl0eSBwb3NpdGlvbiBpbiBhIFRPTkNPIHdUVG9uLVVTROKCriBwb29sLiBUaGUgb3duZXIgb2YgdGhpcyBORlQgY2FuIG1vZGlmeSBvciBjbGFpbSB0aGUgcmV3YXIMAf5kcy4KUG9vbCBBZGRyZXNzOiBFUUQyNXZTdEV3Yy1oMVFUMXFsc1lQUXdxVTVJaU9ob3g1SUkwQ194c0ROcE1WbzcKd1RUb24gTWFzdGVyIEFkZHJlc3M6IEVRQ1VuRXhtZGd3QUtBRGktajJLUEtUaHlRcVRjN1U2NTBjZ00wDQCmZzc4VXpaWG45SgpVU0Tigq4gTWFzdGVyIEFkZHJlc3M6IEVRQ3hFNm1VdFFKS0ZuR2ZhUk9US090MWxaYkRpaVgxa0NpeFJ2N053MklkX3NEcwoBQb9jabZt8hHKr8RkIu1tpovrTgGp91LycCwKZP+4ORW37w8AVgBbeyJ0cmFpdF90eXBlIjogIkRFWCIsICJ2YWx1ZSI6ICJUT05DTyIgfV0="))
    assert metadata == {
        'name': 'Pool wTTon-USD₮ Position',
        'description': '\x00#38 LP Position: [ -887220 -> 887220 ]\nThis NFT represents a liquidity position in a TONCO wTTon-USD₮ pool. The owner of this NFT can modify or claim the rewards.\nPool Address: EQD25vStEwc-h1QT1qlsYPQwqU5IiOhox5II0C_xsDNpMVo7\nwTTon Master Address: EQCUnExmdgwAKADi-j2KPKThyQqTc7U650cgM0g78UzZXn9J\nUSD₮ Master Address: EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs\n',
        'attributes': '[{"trait_type": "DEX", "value": "TONCO" }]', 'image': 'https://tonco.io/static/tonco-logo-nft.png'
    }

def test_onchain_metadata_chunked():
    metadata = parser.parse_metadata(Cell.one_from_boc("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"))
    assert metadata == {
        'name': 'Magnetic Meridian #12846',
        'description': 'Random Meridian character generated while mining $MRDN token',
        'image': 'data:image/svg+xml;utf8,<svg viewBox="0 0 160 160" width="160" height="160" xmlns="http://www.w3.org/2000/svg" shape-rendering="crispEdges"><path fill="rgb(213,228,229)" d="M0 0 h160v10h-160z"/><path fill="rgb(213,228,229)" d="M0 10 h160v10h-160z"/><path fill="rgb(213,228,229)" d="M0 20 h160v10h-160z"/><path fill="rgb(213,228,229)" d="M0 30 h160v10h-160z"/><path fill="rgb(213,228,229)" d="M0 40 h160v10h-160z"/><path fill="rgb(213,228,229)" d="M0 50 h160v10h-160z"/><path fill="rgb(213,228,229)" d="M0 60 h160v10h-160z"/><path fill="rgb(213,228,229)" d="M0 70 h160v10h-160z"/><path fill="rgb(213,228,229)" d="M0 80 h160v10h-160z"/><path fill="rgb(213,228,229)" d="M0 90 h160v10h-160z"/><path fill="rgb(213,228,229)" d="M0 100 h160v10h-160z"/><path fill="rgb(213,228,229)" d="M0 110 h160v10h-160z"/><path fill="rgb(213,228,229)" d="M0 120 h160v10h-160z"/><path fill="rgb(213,228,229)" d="M0 130 h160v10h-160z"/><path fill="rgb(213,228,229)" d="M0 140 h160v10h-160z"/><path fill="rgb(213,228,229)" d="M0 150 h160v10h-160z"/>'
    }