"""
Etherscan web scraper using Selenium to extract contract name tags.
"""

import logging
import time
from typing import Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger(__name__)


class EtherscanScraper:
    """
    Web scraper for extracting contract name tags from Etherscan.

    Example:
        >>> scraper = EtherscanScraper()
        >>> name_tag = scraper.get_contract_name_tag("0x94cc50e4521bd271c1a997a3a4dc815c2f920b41")
        >>> print(name_tag)
        'Curve: crvUSDSUSD-f Pool'
        >>> scraper.close()
    """

    BASE_URL = "https://etherscan.io/address/"

    def __init__(self, headless: bool = True, timeout: int = 10):
        """
        Initialize the Etherscan scraper.

        Args:
            headless: Run browser in headless mode (no GUI)
            timeout: Maximum wait time for page elements (seconds)
        """
        self.timeout = timeout
        self.driver = self._setup_driver(headless)
        logger.info("EtherscanScraper initialized")

    def _setup_driver(self, headless: bool) -> webdriver.Chrome:
        """
        Set up Chrome WebDriver with appropriate options.

        Args:
            headless: Run browser in headless mode

        Returns:
            Configured Chrome WebDriver instance
        """
        chrome_options = Options()

        if headless:
            chrome_options.add_argument("--headless")

        # Additional options for better performance and reliability
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        # Set up Chrome driver with automatic driver management
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)

        return driver

    def get_contract_name_tag(self, address: str) -> Optional[str]:
        """
        Extract the name tag for a given contract address.

        Args:
            address: Ethereum contract address (with or without '0x' prefix)

        Returns:
            Contract name tag if found, None otherwise

        Example:
            >>> scraper = EtherscanScraper()
            >>> name = scraper.get_contract_name_tag("0x94cc50e4521bd271c1a997a3a4dc815c2f920b41")
            >>> print(name)
            'Curve: crvUSDSUSD-f Pool'
        """
        # Normalize address
        if not address.startswith("0x"):
            address = f"0x{address}"

        address = address.lower()
        url = f"{self.BASE_URL}{address}"

        logger.info(f"Fetching name tag for address: {address}")

        try:
            # Navigate to the address page
            self.driver.get(url)

            # Wait for the page to load
            time.sleep(1)

            # Try to find the name tag element
            # The name tag appears in a span with class "hash-tag text-truncate"
            try:
                wait = WebDriverWait(self.driver, self.timeout)
                name_tag_element = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "span.hash-tag.text-truncate"))
                )

                name_tag = name_tag_element.text.strip()
                logger.info(f"Found name tag: {name_tag}")
                return name_tag

            except TimeoutException:
                logger.warning(f"No name tag found for address {address} (element not found within timeout)")
                return None
            except NoSuchElementException:
                logger.warning(f"No name tag found for address {address} (element does not exist)")
                return None

        except Exception as e:
            logger.error(f"Error scraping address {address}: {e}")
            return None

    def get_contract_info(self, address: str) -> dict:
        """
        Extract comprehensive contract information including name tag and other details.

        Args:
            address: Ethereum contract address

        Returns:
            Dictionary containing contract information

        Example:
            >>> scraper = EtherscanScraper()
            >>> info = scraper.get_contract_info("0x94cc50e4521bd271c1a997a3a4dc815c2f920b41")
            >>> print(info['name_tag'])
            'Curve: crvUSDSUSD-f Pool'
        """
        name_tag = self.get_contract_name_tag(address)

        return {
            "address": address.lower(),
            "name_tag": name_tag,
            "has_name_tag": name_tag is not None,
            "url": f"{self.BASE_URL}{address.lower()}"
        }

    def close(self):
        """Close the WebDriver and clean up resources."""
        if self.driver:
            self.driver.quit()
            logger.info("EtherscanScraper closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
