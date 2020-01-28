import asyncio
import os
import tempfile

try:
    # These dependencies are optional. The storage component may be used
    # without the screenshot component.
    from jinja2 import Template
    from pyppeteer import launch
    from bokeh.resources import CDN

    BASE_ARGS = {
        "bokeh_scripts": {"cdn_js": CDN.js_files[0], "widget_js": CDN.js_files[1]}
    }
    SCREENSHOT_ENABLED = True

except ImportError:
    SCREENSHOT_ENABLED = False
    Template = None
    launch = None
    CDN = None
    BASE_ARGS = {}

import cs_storage


CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))


class ScreenshotError(Exception):
    pass


def get_template():
    if not SCREENSHOT_ENABLED:
        return None
    with open(f"{CURRENT_DIR}/templates/index.html", "r") as f:
        text = f.read()

    template = Template(text)

    return template


TEMPLATE = get_template()


def write_template(output):
    kwargs = {**BASE_ARGS, **{"output": output}}
    return TEMPLATE.render(**kwargs)


async def _screenshot(template_path, pic_path):
    """
    Use pyppeteer, a python port of puppeteer, to open the
    template at template_path and take a screenshot of the
    output that is rendered within it.

    The output is rendered within a Bootstrap card element.
    This element is only as big as the elements that it contains.
    Thus, we only need to get the dimensions of the bootstrap
    card to figure out which part of the screen we need to use
    for the screenshot!

    Note: pyppetter looks stale. If it continues to not be
    maintained well, then the extremely active, well-maintained
    puppeteer should be used for creating these screenshots. The
    downside of using puppeteer is that it is written in nodejs.
    """
    browser = await launch(
        handleSIGINT=False,
        handleSIGTERM=False,
        handleSIGHUP=False,
        args=["--no-sandbox"],
    )
    page = await browser.newPage()
    await page.goto(f"file://{template_path}")
    await page.setViewport(dict(width=1920, height=1080))
    await page.waitFor(1000)
    element = await page.querySelector("#output")
    if element is None:
        raise ScreenshotError("Unable to take screenshot.")
    boundingbox = await element.boundingBox()
    clip = dict(
        x=boundingbox["x"],
        y=boundingbox["y"],
        width=min(boundingbox["width"], 1920),
        height=min(boundingbox["height"], 1080),
    )
    await page.screenshot(path=f"{pic_path}", type="png", clip=clip)
    await browser.close()


def screenshot(output, debug=False):
    """
    Create screenshot of outputs. The intermediate results are
    written to temporary files and a picture, represented as a
    stream of bytes, is returned.
    """
    if not SCREENSHOT_ENABLED:
        return None
    html = write_template(output)
    with tempfile.NamedTemporaryFile(suffix=".html") as temp:
        if debug:
            with open(f'{output["title"]}.html', "w") as f:
                f.write(html)
        temp.write(html.encode("utf-8"))
        temp.seek(0)
        template_path = temp.name
        with tempfile.NamedTemporaryFile(suffix=".png") as pic:
            pic_path = pic.name
            asyncio.get_event_loop().run_until_complete(
                _screenshot(template_path, pic_path)
            )
            pic_bytes = pic.read()
    return pic_bytes
