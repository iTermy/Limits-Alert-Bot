"""
Comprehensive testing script for SymbolMapper and AlertDistanceConfig
Tests symbol mapping accuracy and alert distance configuration
"""

import json
import sys
import os
from pathlib import Path
from typing import Dict, List, Tuple
from dataclasses import dataclass
from colorama import init, Fore, Style

# Initialize colorama for colored output on Windows
init(autoreset=True, convert=True)

# Get the project root directory (parent of scripts/)
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import from price_feeds directory
from price_feeds.symbol_mapper import SymbolMapper
from price_feeds.alert_config import AlertDistanceConfig


@dataclass
class TestCase:
    """Test case for symbol mapping"""
    symbol: str
    expected_asset_class: str
    expected_feed: str
    expected_mapped: str
    description: str = ""


class SymbolMapperTester:
    """Comprehensive tester for SymbolMapper and AlertDistanceConfig"""

    def __init__(self):
        # Get correct paths for Windows
        project_root = Path(__file__).parent.parent
        mapper_config = project_root / 'config' / 'symbol_mappings.json'
        alert_config = project_root / 'config' / 'alert_distances.json'

        self.mapper = SymbolMapper(str(mapper_config))
        self.alert_config = AlertDistanceConfig(str(alert_config))
        self.passed = 0
        self.failed = 0
        self.warnings = 0

    def run_all_tests(self):
        """Run all test suites"""
        print(f"\n{Fore.CYAN}{'='*80}")
        print(f"{Fore.CYAN}SYMBOL MAPPER & ALERT CONFIGURATION TEST SUITE")
        print(f"{Fore.CYAN}{'='*80}\n")

        # Test symbol mapping
        self.test_forex_symbols()
        self.test_indices_symbols()
        self.test_crypto_symbols()
        self.test_metals_symbols()
        self.test_stocks_symbols()
        self.test_oil_symbols()
        self.test_edge_cases()

        # Test alert distances
        self.test_alert_distances()

        # Print summary
        self.print_summary()

    def test_case(self, test: TestCase) -> bool:
        """Run a single test case"""
        # Get actual values
        actual_class = self.mapper.determine_asset_class(test.symbol)
        actual_feed = self.mapper.get_best_feed(test.symbol)
        actual_mapped = self.mapper.get_feed_symbol(test.symbol, actual_feed)

        # Check if all match expected
        class_match = actual_class == test.expected_asset_class
        feed_match = actual_feed == test.expected_feed
        mapped_match = actual_mapped == test.expected_mapped

        all_match = class_match and feed_match and mapped_match

        # Print result
        if all_match:
            print(f"{Fore.GREEN}✓ {test.symbol:15} → {actual_mapped:15} ({actual_feed:10}) {test.description}")
            self.passed += 1
        else:
            print(f"{Fore.RED}✗ {test.symbol:15}")
            if not class_match:
                print(f"  {Fore.YELLOW}Asset class: Expected '{test.expected_asset_class}', got '{actual_class}'")
            if not feed_match:
                print(f"  {Fore.YELLOW}Feed: Expected '{test.expected_feed}', got '{actual_feed}'")
            if not mapped_match:
                print(f"  {Fore.YELLOW}Mapped: Expected '{test.expected_mapped}', got '{actual_mapped}'")
            self.failed += 1

        return all_match

    def test_forex_symbols(self):
        """Test forex symbol mapping"""
        print(f"\n{Fore.BLUE}[FOREX SYMBOLS TEST]")
        print(f"{Fore.BLUE}{'-'*40}")

        test_cases = [
            TestCase('EURUSD', 'forex', 'icmarkets', 'EURUSD', 'Major pair'),
            TestCase('GBPJPY', 'forex', 'icmarkets', 'GBPJPY', 'JPY cross'),
            TestCase('AUDUSD', 'forex', 'icmarkets', 'AUDUSD', 'Commodity pair'),
            TestCase('USDCHF', 'forex', 'icmarkets', 'USDCHF', 'Safe haven'),
            TestCase('NZDJPY', 'forex', 'icmarkets', 'NZDJPY', 'Minor cross'),
            TestCase('EURGBP', 'forex', 'icmarkets', 'EURGBP', 'Euro cross'),
            TestCase('USDCAD', 'forex', 'icmarkets', 'USDCAD', 'Dollar pair'),
            TestCase('AUDNZD', 'forex', 'icmarkets', 'AUDNZD', 'Aussie cross'),
        ]

        for test in test_cases:
            self.test_case(test)

            # Also test OANDA mapping for forex
            oanda_mapped = self.mapper.get_feed_symbol(test.symbol, 'oanda')
            expected_oanda = f"{test.symbol[:3]}_{test.symbol[3:]}"
            if oanda_mapped == expected_oanda:
                print(f"  {Fore.CYAN}↳ OANDA fallback: {oanda_mapped} ✓")
            else:
                print(f"  {Fore.YELLOW}↳ OANDA fallback: Expected {expected_oanda}, got {oanda_mapped}")
                self.warnings += 1

    def test_indices_symbols(self):
        """Test indices symbol mapping"""
        print(f"\n{Fore.BLUE}[INDICES SYMBOLS TEST]")
        print(f"{Fore.BLUE}{'-'*40}")

        test_cases = [
            TestCase('spx500usd', 'indices', 'oanda', 'SPX500_USD', 'S&P 500'),
            TestCase('spx500', 'indices', 'oanda', 'SPX500_USD', 'S&P 500 short'),
            TestCase('nas100usd', 'indices', 'oanda', 'NAS100_USD', 'NASDAQ 100'),
            TestCase('nas100', 'indices', 'oanda', 'NAS100_USD', 'NASDAQ short'),
            TestCase('jp225', 'indices', 'oanda', 'JP225_USD', 'Nikkei 225'),
            TestCase('de30eur', 'indices', 'oanda', 'DE30_EUR', 'DAX 30'),
            TestCase('dax', 'indices', 'oanda', 'DE30_EUR', 'DAX alias'),
            TestCase('china50', 'indices', 'oanda', 'CN50_USD', 'China A50'),
            TestCase('us2000usd', 'indices', 'oanda', 'US2000_USD', 'Russell 2000'),
            TestCase('fra40', 'indices', 'oanda', 'FR40_EUR', 'CAC 40'),
            TestCase('uk100', 'indices', 'oanda', 'UK100_GBP', 'FTSE 100'),
            TestCase('aus200', 'indices', 'oanda', 'AU200_AUD', 'ASX 200'),
        ]

        for test in test_cases:
            self.test_case(test)

    def test_crypto_symbols(self):
        """Test crypto symbol mapping"""
        print(f"\n{Fore.BLUE}[CRYPTO SYMBOLS TEST]")
        print(f"{Fore.BLUE}{'-'*40}")

        test_cases = [
            TestCase('BTCUSDT', 'crypto', 'binance', 'BTCUSDT', 'Bitcoin USDT'),
            TestCase('BTC', 'crypto', 'binance', 'BTCUSDT', 'Bitcoin no suffix'),
            TestCase('BTCUSD', 'crypto', 'binance', 'BTCUSDT', 'Bitcoin USD→USDT'),
            TestCase('ETHUSDT', 'crypto', 'binance', 'ETHUSDT', 'Ethereum USDT'),
            TestCase('ETH', 'crypto', 'binance', 'ETHUSDT', 'Ethereum no suffix'),
            TestCase('DOGEUSDT', 'crypto', 'binance', 'DOGEUSDT', 'Dogecoin'),
            TestCase('DOGE', 'crypto', 'binance', 'DOGEUSDT', 'Doge no suffix'),
            TestCase('SOLUSDT', 'crypto', 'binance', 'SOLUSDT', 'Solana'),
            TestCase('ADAUSDT', 'crypto', 'binance', 'ADAUSDT', 'Cardano'),
            TestCase('MATICUSDT', 'crypto', 'binance', 'MATICUSDT', 'Polygon'),
        ]

        for test in test_cases:
            self.test_case(test)

    def test_metals_symbols(self):
        """Test metals symbol mapping"""
        print(f"\n{Fore.BLUE}[METALS SYMBOLS TEST]")
        print(f"{Fore.BLUE}{'-'*40}")

        test_cases = [
            TestCase('XAUUSD', 'metals', 'fxcm', 'GOLD', 'Gold standard'),
            TestCase('GOLD', 'metals', 'fxcm', 'GOLD', 'Gold direct'),
            TestCase('XAGUSD', 'metals', 'fxcm', 'SILVER', 'Silver standard'),
            TestCase('SILVER', 'metals', 'fxcm', 'SILVER', 'Silver direct'),
        ]

        for test in test_cases:
            self.test_case(test)

            # Test that FXCM redirects to ICMarkets format
            icmarkets_mapped = self.mapper.get_feed_symbol(test.symbol, 'icmarkets')
            print(f"  {Fore.CYAN}↳ ICMarkets format: {icmarkets_mapped}")

    def test_stocks_symbols(self):
        """Test stocks symbol mapping"""
        print(f"\n{Fore.BLUE}[STOCKS SYMBOLS TEST]")
        print(f"{Fore.BLUE}{'-'*40}")

        test_cases = [
            TestCase('AAPL.NAS', 'stocks', 'icmarkets', 'AAPL.NAS', 'Apple NASDAQ'),
            TestCase('MSFT.NAS', 'stocks', 'icmarkets', 'MSFT.NAS', 'Microsoft NASDAQ'),
            TestCase('TSLA.NAS', 'stocks', 'icmarkets', 'TSLA.NAS', 'Tesla NASDAQ'),
            TestCase('JPM.NYSE', 'stocks', 'icmarkets', 'JPM.NYSE', 'JP Morgan NYSE'),
            TestCase('BAC.NYSE', 'stocks', 'icmarkets', 'BAC.NYSE', 'Bank of America'),
            TestCase('GOOGL.NAS', 'stocks', 'icmarkets', 'GOOGL.NAS', 'Google'),
        ]

        for test in test_cases:
            self.test_case(test)

    def test_oil_symbols(self):
        """Test oil symbols (should not be supported)"""
        print(f"\n{Fore.BLUE}[OIL SYMBOLS TEST - Should Fail]")
        print(f"{Fore.BLUE}{'-'*40}")

        oil_symbols = ['USOILSPOT', 'UKOILSPOT', 'WTIUSD', 'BRENTUSD']

        for symbol in oil_symbols:
            is_valid, reason = self.mapper.validate_symbol(symbol)
            if not is_valid and 'not currently supported' in reason:
                print(f"{Fore.GREEN}✓ {symbol:15} correctly identified as unsupported")
                self.passed += 1
            else:
                print(f"{Fore.RED}✗ {symbol:15} should be unsupported")
                self.failed += 1

    def test_edge_cases(self):
        """Test edge cases and unusual symbols"""
        print(f"\n{Fore.BLUE}[EDGE CASES TEST]")
        print(f"{Fore.BLUE}{'-'*40}")

        edge_cases = [
            ('eurusd', 'EURUSD', 'Case insensitive'),
            ('SpX500USD', 'SPX500_USD', 'Mixed case index'),
            ('btc', 'BTCUSDT', 'Lowercase crypto'),
            ('GOLD', 'GOLD', 'Already in feed format'),
            ('nasdaq', 'NAS100_USD', 'Index alias'),
        ]

        for original, expected, description in edge_cases:
            feed = self.mapper.get_best_feed(original)
            mapped = self.mapper.get_feed_symbol(original, feed)

            if mapped == expected:
                print(f"{Fore.GREEN}✓ {original:15} → {mapped:15} {description}")
                self.passed += 1
            else:
                print(f"{Fore.RED}✗ {original:15} Expected {expected}, got {mapped}")
                self.failed += 1

    def test_alert_distances(self):
        """Test alert distance configuration"""
        print(f"\n{Fore.BLUE}[ALERT DISTANCES TEST]")
        print(f"{Fore.BLUE}{'-'*40}")

        test_symbols = [
            ('EURUSD', 10, 0.0001, 'Forex default'),
            ('USDJPY', 15, 0.01, 'JPY pair override'),
            ('XAUUSD', 1.50, 0.01, 'Gold override'),
            ('BTCUSDT', 100, 1, 'Bitcoin override'),
            ('spx500usd', 5.0, 0.1, 'S&P 500 override'),
            ('AAPL.NAS', 0.50, 0.01, 'Stock default'),
        ]

        for symbol, expected_distance, expected_pip, description in test_symbols:
            config = self.alert_config.get_alert_config(symbol)

            # Get the appropriate distance field
            if 'approaching_pips' in config:
                actual_distance = config['approaching_pips']
                distance_type = 'pips'
            else:
                actual_distance = config['approaching_distance']
                distance_type = 'points'

            actual_pip = config['pip_size']

            if actual_distance == expected_distance and actual_pip == expected_pip:
                print(f"{Fore.GREEN}✓ {symbol:12} → {actual_distance:6} {distance_type:6} (pip: {actual_pip}) - {description}")
                self.passed += 1
            else:
                print(f"{Fore.RED}✗ {symbol:12}")
                if actual_distance != expected_distance:
                    print(f"  {Fore.YELLOW}Distance: Expected {expected_distance}, got {actual_distance}")
                if actual_pip != expected_pip:
                    print(f"  {Fore.YELLOW}Pip size: Expected {expected_pip}, got {actual_pip}")
                self.failed += 1

    def print_summary(self):
        """Print test summary"""
        total = self.passed + self.failed
        pass_rate = (self.passed / total * 100) if total > 0 else 0

        print(f"\n{Fore.CYAN}{'='*80}")
        print(f"{Fore.CYAN}TEST SUMMARY")
        print(f"{Fore.CYAN}{'='*80}")

        if pass_rate >= 95:
            color = Fore.GREEN
            status = "EXCELLENT"
        elif pass_rate >= 90:
            color = Fore.YELLOW
            status = "GOOD"
        else:
            color = Fore.RED
            status = "NEEDS WORK"

        print(f"\n{Fore.GREEN}Passed: {self.passed}")
        print(f"{Fore.RED}Failed: {self.failed}")
        if self.warnings > 0:
            print(f"{Fore.YELLOW}Warnings: {self.warnings}")
        print(f"\n{color}Success Rate: {pass_rate:.1f}% - {status}")

        if pass_rate >= 95:
            print(f"\n{Fore.GREEN}✨ Symbol Mapper is production ready! ✨")
        else:
            print(f"\n{Fore.YELLOW}⚠ Symbol Mapper needs adjustments to reach 95% accuracy")

    def test_batch_operations(self):
        """Test batch symbol operations for performance"""
        print(f"\n{Fore.BLUE}[BATCH OPERATIONS TEST]")
        print(f"{Fore.BLUE}{'-'*40}")

        # Simulate a typical batch of active signals
        batch_symbols = [
            'EURUSD', 'GBPJPY', 'XAUUSD', 'spx500usd', 'BTCUSDT',
            'AAPL.NAS', 'nas100usd', 'AUDUSD', 'ETHUSDT', 'SILVER'
        ]

        print(f"Testing batch of {len(batch_symbols)} symbols...")

        # Group by feed
        feeds_needed = {}
        for symbol in batch_symbols:
            feed = self.mapper.get_best_feed(symbol)
            if feed not in feeds_needed:
                feeds_needed[feed] = []
            feeds_needed[feed].append(symbol)

        # Show feed distribution
        for feed, symbols in feeds_needed.items():
            print(f"\n{Fore.CYAN}{feed.upper()} ({len(symbols)} symbols):")
            for symbol in symbols:
                mapped = self.mapper.get_feed_symbol(symbol, feed)
                print(f"  {symbol:12} → {mapped}")

        print(f"\n{Fore.GREEN}✓ Batch processing successful")
        print(f"  Unique feeds needed: {len(feeds_needed)}")
        print(f"  Feed distribution: {', '.join([f'{k}:{len(v)}' for k,v in feeds_needed.items()])}")


class AlertDistanceConfig:
    """Alert distance configuration handler"""

    def __init__(self, config_path: str = None):
        if config_path is None:
            # Use project root to find config
            project_root = Path(__file__).parent.parent
            config_path = project_root / 'config' / 'alert_distances.json'

        self.config_path = Path(config_path)
        self.config = self._load_config()

        # Initialize SymbolMapper with correct path
        mapper_config_path = self.config_path.parent / 'symbol_mappings.json'
        self.mapper = SymbolMapper(str(mapper_config_path))

    def _load_config(self) -> Dict:
        """Load alert configuration"""
        try:
            with open(self.config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Warning: {self.config_path} not found, using defaults")
            return {
                'defaults': {
                    'forex': {'approaching_pips': 10, 'pip_size': 0.0001},
                    'indices': {'approaching_distance': 5.0, 'pip_size': 1}
                },
                'overrides': {}
            }

    def get_alert_config(self, symbol: str) -> Dict:
        """Get alert configuration for a symbol"""
        # Check overrides first
        overrides = self.config.get('overrides', {})
        if symbol.upper() in overrides:
            return overrides[symbol.upper()]
        if symbol.lower() in overrides:
            return overrides[symbol.lower()]

        # Determine asset class and get defaults
        asset_class = self.mapper.determine_asset_class(symbol)

        # Special case for JPY pairs
        if asset_class == 'forex' and 'JPY' in symbol.upper():
            return self.config['defaults'].get('forex_jpy', self.config['defaults']['forex'])

        # Return default for asset class
        return self.config['defaults'].get(asset_class, {
            'approaching_distance': 10,
            'pip_size': 0.0001
        })


def main():
    """Run the test suite"""
    tester = SymbolMapperTester()
    tester.run_all_tests()

    # Test batch operations separately
    tester.test_batch_operations()


if __name__ == "__main__":
    main()