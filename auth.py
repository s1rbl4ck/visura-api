from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError
import os
import re

# Constants
LOGIN_URL = "https://iampe.agenziaentrate.gov.it/sam/UI/Login?realm=/agenziaentrate"


def _get_and_validate_credentials(method: str) -> tuple[str, str]:
    """Extract and validate ADE credentials from environment."""
    ade_username = os.getenv("ADE_USERNAME")
    ade_password = os.getenv("ADE_PASSWORD")

    if not ade_username or not ade_password:
        raise ValueError(f"ADE_USERNAME and ADE_PASSWORD environment variables must be set for {method} login")

    return ade_username, ade_password


async def _navigate_to_login(page: Page, method: str) -> None:
    """Navigate to the Agenzia delle Entrate login page."""
    print(f"[LOGIN][{method}] Navigating alla page di login...")
    await page.goto(LOGIN_URL)


async def _click_with_fallback(
    page: Page,
    name_pattern,
    fallback_selector: str,
    method: str,
    action_name: str,
    timeout: int = 20000,
    role: str = None
) -> None:
    """Attempt to click an element with fallback selector.
    
    Args:
        page: Playwright page object
        name_pattern: String or regex pattern for element name
        fallback_selector: CSS/Playwright locator fallback
        method: Login method (for logging)
        action_name: Action description (for logging)
        timeout: Click timeout in ms
        role: Specific HTML role to try first (e.g., 'tab', 'button')
    """
    if role:
        # Try specific role first
        try:
            await page.get_by_role(role, name=name_pattern).click(timeout=timeout)
            return
        except PlaywrightTimeoutError:
            pass
    
    # Try button then link (default behavior)
    try:
        await page.get_by_role("button", name=name_pattern).click(timeout=timeout)
        return
    except PlaywrightTimeoutError:
        pass
    
    try:
        await page.get_by_role("link", name=name_pattern).click(timeout=timeout)
        return
    except PlaywrightTimeoutError:
        pass
    
    # Final fallback to locator
    print(f"[LOGIN][{method}] {action_name} - trying fallback selector...")
    await page.locator(fallback_selector).first.click(timeout=timeout)


async def login(page: Page):
    login_method = os.getenv("ADE_LOGIN_METHOD", "CIE").strip().upper()

    if login_method == "CIE":
        await login_cie(page)
    elif login_method == "SPID":
        await login_spid(page)
    else:
        raise ValueError("ADE_LOGIN_METHOD must be either 'CIE' or 'SPID'")

    print(f"[LOGIN][{login_method}] accesso riuscito.")

    # Common post-login flow for both methods
    await _open_sister_after_auth(page)


async def login_spid(page: Page):
    method = "SPID"
    ade_username, ade_password = _get_and_validate_credentials(method)

    await _navigate_to_login(page, method)

    print(f"[LOGIN][{method}] Clicking 'Entra con SPID'...")
    await page.get_by_role("button", name="Entra con SPID").click()

    print(f"[LOGIN][{method}] Clicking 'Sielte ID'...")
    await page.get_by_role("link", name="Sielte ID").click()

    print(f"[LOGIN][{method}] Entering username...")
    await page.get_by_role("textbox", name="Codice Fiscale / Partita IVA").fill(ade_username)

    print(f"[LOGIN][{method}] Entering password...")
    await page.get_by_role("textbox", name="Password").click()
    await page.get_by_role("textbox", name="Password").fill(ade_password)

    print(f"[LOGIN][{method}] Clicking 'Prosegui'...")
    await page.get_by_role("button", name="Prosegui").click()

    print(f"[LOGIN][{method}] Searching for link notifica (può non esserci)...")
    try:
        await _click_with_fallback(
            page,
            "Utilizza il le notifiche Ricevi una notifica sull'app MySielteID",
            'a.link-sso:has(img[alt="Utilizza il le notifiche"]):has(p:text("Ricevi una notifica sull\'app MySielteID"))',
            method,
            "link notifica",
            timeout=4000,
            role="link"
        )
        print(f"[LOGIN][{method}] clicked link notifica (found).")
    except PlaywrightTimeoutError:
        print(f"[LOGIN][{method}] No link notifica found, continuing anyway.")

    print(f"[LOGIN][{method}] Clicking 'Autorizza'...")
    await page.get_by_role("button", name="Autorizza").click()


async def login_cie(page: Page):
    method = "CIE"
    ade_username, ade_password = _get_and_validate_credentials(method)

    await _navigate_to_login(page, method)

    print(f"[LOGIN][{method}] Selecting tab CIE...")
    await _click_with_fallback(
        page,
        "CIE",
        "a[role='tab'][aria-controls='tab-2']",
        method,
        "Selecting tab CIE",
        timeout=15000,
        role="tab"
    )

    print(f"[LOGIN][{method}] Clicking 'Entra con CIE'...")
    await _click_with_fallback(
        page,
        re.compile("Entra con CIE", re.IGNORECASE),
        "a:has-text('Entra con CIE'), button:has-text('Entra con CIE')",
        method,
        "Clicking 'Entra con CIE'",
        timeout=20000
    )

    print(f"[LOGIN][{method}] Entering username...")
    await page.locator("input#username[name='username']").fill(ade_username)

    print(f"[LOGIN][{method}] Entering password...")
    await page.locator("input#password[name='password']").fill(ade_password)

    print(f"[LOGIN][{method}] Clicking 'Procedi'...")
    await page.locator("button[type='submit']").first.click()

    print(f"[LOGIN][{method}] Waiting for mobile authorization confirmation...")

    print(f"[LOGIN][{method}] Clicking 'Prosegui' after authorization...")
    try:
        await page.locator("button[type='submit'][name='_eventId_proceed']").click(timeout=120000)
    except PlaywrightTimeoutError:
        await page.get_by_role("button", name="Prosegui").click(timeout=120000)

async def _open_sister_after_auth(page: Page):
    print("[LOGIN] Cerco servizio SISTER...")
    await page.get_by_role("textbox", name="Cerca il servizio").click()
    await page.get_by_role("textbox", name="Cerca il servizio").fill("SISTER")
    await page.get_by_role("textbox", name="Cerca il servizio").press("Enter")
    print("[LOGIN] Clicco 'Vai al servizio'...")
    await page.get_by_role("link", name="Vai al servizio").first.click()

    print("[LOGIN] Attendo caricamento pagina...")
    await page.wait_for_load_state("networkidle")
    print("[LOGIN] Controllo blocco sessione...")
    content = await page.content()
    url = page.url
    if (
        "Utente gia' in sessione" in content
        or "error_locked.jsp" in url
    ):
        print("[LOGIN][ERRORE] Utente già in sessione su un'altra postazione!")
        raise Exception("Utente già in sessione su un'altra postazione")

    print("[LOGIN] Clicco 'Conferma'...")
    await page.get_by_role("button", name="Conferma").click()
    print("[LOGIN] Clicco 'Consultazioni e Certificazioni'...")
    await page.get_by_role("link", name="Consultazioni e Certificazioni").click()
    print("[LOGIN] Clicco 'Visure catastali'...")
    await page.get_by_role("link", name="Visure catastali").click()
    print("[LOGIN] Clicco 'Conferma Lettura'...")
    await page.get_by_role("link", name="Conferma Lettura").click()
