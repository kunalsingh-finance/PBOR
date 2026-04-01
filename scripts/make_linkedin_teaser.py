from __future__ import annotations

import math
import os
import subprocess
import textwrap
from dataclasses import dataclass
from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps

try:
    import imageio_ffmpeg
except ImportError as exc:  # pragma: no cover - local utility script
    raise SystemExit(
        "Install imageio-ffmpeg first: python -m pip install --user imageio-ffmpeg"
    ) from exc


WIDTH = 1080
HEIGHT = 1920
FPS = 24
CANVAS = (WIDTH, HEIGHT)
VIDEO_SECONDS = 21.0
BACKGROUND = "#08111F"
PANEL = "#102033"
PANEL_EDGE = "#23476F"
TEXT_MAIN = "#F3F7FB"
TEXT_MUTED = "#B5C6DA"
ACCENT = "#63E6D5"
ACCENT_ALT = "#6BA7FF"
WARNING = "#F4C34B"


@dataclass(frozen=True)
class Scene:
    duration: float
    renderer: callable


def ease(progress: float) -> float:
    progress = max(0.0, min(1.0, progress))
    return 0.5 - 0.5 * math.cos(math.pi * progress)


def hex_rgba(value: str, alpha: int = 255) -> tuple[int, int, int, int]:
    value = value.lstrip("#")
    return tuple(int(value[i : i + 2], 16) for i in (0, 2, 4)) + (alpha,)


def pick_font_path(bold: bool) -> str | None:
    windir = Path(os.environ.get("WINDIR", "C:/Windows"))
    candidates = [
        windir / "Fonts" / ("segoeuib.ttf" if bold else "segoeui.ttf"),
        windir / "Fonts" / ("arialbd.ttf" if bold else "arial.ttf"),
        windir / "Fonts" / ("calibrib.ttf" if bold else "calibri.ttf"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return None


FONT_PATH_BOLD = pick_font_path(bold=True)
FONT_PATH_REG = pick_font_path(bold=False)


def load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    font_path = FONT_PATH_BOLD if bold else FONT_PATH_REG
    if font_path:
        return ImageFont.truetype(font_path, size=size)
    return ImageFont.load_default()


TITLE_FONT = load_font(112, bold=True)
TITLE_MID_FONT = load_font(86, bold=True)
SUBTITLE_FONT = load_font(48, bold=False)
BODY_FONT = load_font(38, bold=False)
BODY_BOLD = load_font(40, bold=True)
SMALL_FONT = load_font(27, bold=False)
SMALL_BOLD = load_font(28, bold=True)
CHIP_FONT = load_font(28, bold=True)


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


ROOT = project_root()
TEARSHEET_PATH = ROOT / "docs" / "tearsheet-sample.png"
CONTROLS_PATH = ROOT / "outputs" / "2026-01" / "controls_table.png"
OUTPUT_VIDEO = ROOT / "pbor_linkedin_teaser.mp4"
OUTPUT_COVER = ROOT / "pbor_video_cover.png"
OUTPUT_CAPTION = ROOT / "pbor_linkedin_caption.txt"


def wrap_text(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont, max_width: int) -> list[str]:
    words = text.split()
    lines: list[str] = []
    current = ""
    for word in words:
        trial = f"{current} {word}".strip()
        if draw.textbbox((0, 0), trial, font=font)[2] <= max_width or not current:
            current = trial
        else:
            lines.append(current)
            current = word
    if current:
        lines.append(current)
    return lines


def draw_text_block(
    image: Image.Image,
    text: str,
    box: tuple[int, int, int, int],
    font: ImageFont.ImageFont,
    fill: str,
    line_gap: int = 10,
    align: str = "left",
    stroke_width: int = 0,
    stroke_fill: str | None = None,
) -> int:
    draw = ImageDraw.Draw(image)
    x0, y0, x1, y1 = box
    max_width = x1 - x0
    lines = wrap_text(draw, text, font, max_width=max_width)
    bbox = draw.textbbox((0, 0), "Ag", font=font, stroke_width=stroke_width)
    line_height = bbox[3] - bbox[1]
    total_height = len(lines) * line_height + max(0, len(lines) - 1) * line_gap
    y = y0 + max(0, (y1 - y0 - total_height) // 2)
    for line in lines:
        line_box = draw.textbbox((0, 0), line, font=font, stroke_width=stroke_width)
        line_width = line_box[2] - line_box[0]
        if align == "center":
            x = x0 + (max_width - line_width) // 2
        elif align == "right":
            x = x1 - line_width
        else:
            x = x0
        draw.text(
            (x, y),
            line,
            font=font,
            fill=fill,
            stroke_width=stroke_width,
            stroke_fill=stroke_fill,
        )
        y += line_height + line_gap
    return y


def make_background(asset: Image.Image, progress: float, blur_radius: int = 18) -> Image.Image:
    scale = 1.03 + 0.06 * ease(progress)
    resized = asset.resize(
        (max(1, int(asset.width * scale)), max(1, int(asset.height * scale))),
        Image.Resampling.LANCZOS,
    )
    frame = ImageOps.fit(resized, CANVAS, method=Image.Resampling.LANCZOS, centering=(0.5, 0.5))
    frame = frame.filter(ImageFilter.GaussianBlur(radius=blur_radius))
    overlay = Image.new("RGBA", CANVAS, hex_rgba(BACKGROUND, 215))
    return Image.alpha_composite(frame.convert("RGBA"), overlay)


def add_vignette(image: Image.Image, top_alpha: int = 90, bottom_alpha: int = 170) -> None:
    gradient = Image.new("L", (1, HEIGHT))
    for y in range(HEIGHT):
        mix = y / max(1, HEIGHT - 1)
        value = int(top_alpha + (bottom_alpha - top_alpha) * mix)
        gradient.putpixel((0, y), value)
    alpha = gradient.resize(CANVAS)
    overlay = Image.new("RGBA", CANVAS, hex_rgba(BACKGROUND, 0))
    overlay.putalpha(alpha)
    image.alpha_composite(overlay)


def add_card(
    canvas: Image.Image,
    image: Image.Image,
    box: tuple[int, int, int, int],
    progress: float = 0.0,
    caption: str | None = None,
    caption_fill: str = TEXT_MUTED,
) -> None:
    x0, y0, x1, y1 = box
    shadow = Image.new("RGBA", CANVAS, (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow)
    shadow_draw.rounded_rectangle(
        (x0 + 10, y0 + 18, x1 + 10, y1 + 18),
        radius=38,
        fill=(0, 0, 0, 120),
    )
    shadow = shadow.filter(ImageFilter.GaussianBlur(radius=18))
    canvas.alpha_composite(shadow)

    panel = Image.new("RGBA", CANVAS, (0, 0, 0, 0))
    panel_draw = ImageDraw.Draw(panel)
    panel_draw.rounded_rectangle(
        box,
        radius=38,
        fill=hex_rgba(PANEL, 255),
        outline=hex_rgba(PANEL_EDGE, 255),
        width=3,
    )
    canvas.alpha_composite(panel)

    inset = 22
    inner_box = (x0 + inset, y0 + inset, x1 - inset, y1 - inset)
    scale = 1.0 + 0.03 * ease(progress)
    scaled = image.resize(
        (max(1, int(image.width * scale)), max(1, int(image.height * scale))),
        Image.Resampling.LANCZOS,
    )
    fitted = ImageOps.fit(scaled, (inner_box[2] - inner_box[0], inner_box[3] - inner_box[1]), Image.Resampling.LANCZOS)
    mask = Image.new("L", fitted.size, 0)
    mask_draw = ImageDraw.Draw(mask)
    mask_draw.rounded_rectangle((0, 0, fitted.size[0], fitted.size[1]), radius=26, fill=255)
    canvas.paste(fitted.convert("RGBA"), (inner_box[0], inner_box[1]), mask)

    if caption:
        draw = ImageDraw.Draw(canvas)
        draw.text((x0 + 26, y1 + 18), caption, font=SMALL_FONT, fill=caption_fill)


def draw_chip(canvas: Image.Image, xy: tuple[int, int], text: str) -> int:
    draw = ImageDraw.Draw(canvas)
    bbox = draw.textbbox((0, 0), text, font=CHIP_FONT)
    pad_x = 24
    pad_y = 14
    width = (bbox[2] - bbox[0]) + pad_x * 2
    height = (bbox[3] - bbox[1]) + pad_y * 2
    x, y = xy
    draw.rounded_rectangle(
        (x, y, x + width, y + height),
        radius=24,
        fill=hex_rgba(PANEL, 255),
        outline=hex_rgba(PANEL_EDGE, 255),
        width=2,
    )
    draw.text((x + pad_x, y + pad_y - 3), text, font=CHIP_FONT, fill=TEXT_MAIN)
    return width


def draw_step_list(canvas: Image.Image, steps: list[str], top_y: int) -> None:
    draw = ImageDraw.Draw(canvas)
    y = top_y
    for index, step in enumerate(steps, start=1):
        circle_x = 110
        circle_size = 56
        draw.ellipse(
            (circle_x, y, circle_x + circle_size, y + circle_size),
            fill=hex_rgba("#12304E", 255),
            outline=hex_rgba(ACCENT, 255),
            width=2,
        )
        number = str(index)
        nb = draw.textbbox((0, 0), number, font=SMALL_BOLD)
        nx = circle_x + (circle_size - (nb[2] - nb[0])) // 2
        ny = y + (circle_size - (nb[3] - nb[1])) // 2 - 2
        draw.text((nx, ny), number, font=SMALL_BOLD, fill=ACCENT)
        draw_text_block(
            canvas,
            step,
            (196, y - 8, WIDTH - 110, y + 78),
            font=BODY_FONT,
            fill=TEXT_MAIN,
        )
        y += 116


def render_hook(assets: dict[str, Image.Image], progress: float) -> Image.Image:
    frame = make_background(assets["tearsheet"], progress)
    add_vignette(frame, top_alpha=105, bottom_alpha=180)
    draw = ImageDraw.Draw(frame)
    draw.text((90, 160), "PBOR-Lite", font=SMALL_BOLD, fill=ACCENT)
    draw_text_block(
        frame,
        "Personal project in portfolio analytics and investment operations",
        (90, 280, WIDTH - 90, 760),
        font=TITLE_FONT,
        fill=TEXT_MAIN,
        line_gap=8,
    )
    draw_text_block(
        frame,
        "PBOR-style month-end reporting workflow simulation",
        (90, 820, WIDTH - 90, 960),
        font=SUBTITLE_FONT,
        fill=TEXT_MUTED,
    )
    draw.rounded_rectangle(
        (90, 1090, WIDTH - 90, 1340),
        radius=36,
        fill=hex_rgba(PANEL, 220),
        outline=hex_rgba(PANEL_EDGE, 255),
        width=2,
    )
    draw_text_block(
        frame,
        "Built with bundled sample inputs and optional public market data.",
        (130, 1135, WIDTH - 130, 1295),
        font=BODY_FONT,
        fill=TEXT_MAIN,
        align="center",
    )
    draw.text((90, 1710), "Clean, grounded, recruiter-friendly walkthrough", font=SMALL_FONT, fill=TEXT_MUTED)
    return frame


def render_overview(assets: dict[str, Image.Image], progress: float) -> Image.Image:
    frame = Image.new("RGBA", CANVAS, hex_rgba(BACKGROUND, 255))
    add_vignette(frame, top_alpha=25, bottom_alpha=120)
    draw_text_block(
        frame,
        "Uses public market data to simulate a month-end reporting workflow",
        (80, 120, WIDTH - 80, 360),
        font=SUBTITLE_FONT,
        fill=TEXT_MAIN,
        align="center",
    )
    chip_y = 410
    chip_x = 110
    chip_gap = 18
    for label in ["Returns", "Attribution", "QA checks", "Reporting outputs"]:
        width = draw_chip(frame, (chip_x, chip_y), label)
        chip_x += width + chip_gap
        if chip_x > WIDTH - 200:
            chip_x = 230
            chip_y += 92
    add_card(
        frame,
        assets["tearsheet"],
        (70, 760, WIDTH - 70, 1490),
        progress=progress,
        caption="Sample report output from the repo",
    )
    draw = ImageDraw.Draw(frame)
    draw.text((90, 1630), "Personal / educational project", font=SMALL_FONT, fill=TEXT_MUTED)
    draw.text((90, 1680), "Built around the repo's existing tear-sheet output", font=SMALL_FONT, fill=TEXT_MUTED)
    return frame


def render_workflow(assets: dict[str, Image.Image], progress: float) -> Image.Image:
    frame = Image.new("RGBA", CANVAS, hex_rgba(BACKGROUND, 255))
    subtle = make_background(assets["controls"], progress, blur_radius=12)
    subtle.putalpha(55)
    frame.alpha_composite(subtle)
    draw = ImageDraw.Draw(frame)
    draw.text((90, 120), "Workflow", font=TITLE_FONT, fill=TEXT_MAIN)
    draw_text_block(
        frame,
        "How the month-end review pack comes together",
        (90, 270, WIDTH - 90, 360),
        font=SUBTITLE_FONT,
        fill=TEXT_MUTED,
    )
    steps = [
        "Ingest inputs",
        "Calculate returns",
        "Run attribution",
        "Apply QA / reconciliation checks",
        "Generate reporting outputs",
    ]
    draw_step_list(frame, steps, top_y=450)
    add_card(
        frame,
        assets["controls"],
        (140, 1370, WIDTH - 140, 1700),
        progress=progress,
        caption="Control snapshot used in the sample output pack",
    )
    return frame


def render_why(assets: dict[str, Image.Image], progress: float) -> Image.Image:
    frame = make_background(assets["tearsheet"], progress, blur_radius=20)
    add_vignette(frame, top_alpha=140, bottom_alpha=205)
    add_card(frame, assets["controls"], (120, 180, WIDTH - 120, 560), progress=progress)
    draw_text_block(
        frame,
        "Built to better understand the control layer around performance reporting",
        (110, 700, WIDTH - 110, 1210),
        font=TITLE_MID_FONT,
        fill=TEXT_MAIN,
        align="center",
        line_gap=4,
    )
    draw_text_block(
        frame,
        "How inputs, return math, attribution, and QA checks connect at month-end.",
        (120, 1325, WIDTH - 120, 1465),
        font=BODY_FONT,
        fill=TEXT_MUTED,
        align="center",
    )
    return frame


def render_close(assets: dict[str, Image.Image], progress: float) -> Image.Image:
    frame = Image.new("RGBA", CANVAS, hex_rgba(BACKGROUND, 255))
    add_card(frame, assets["tearsheet"], (170, 120, WIDTH - 170, 620), progress=progress)
    draw = ImageDraw.Draw(frame)
    draw_text_block(
        frame,
        "Python  •  pandas  •  SQL",
        (90, 760, WIDTH - 90, 910),
        font=SUBTITLE_FONT,
        fill=TEXT_MAIN,
        align="center",
    )
    draw_text_block(
        frame,
        "SQLite  •  Streamlit",
        (90, 900, WIDTH - 90, 1010),
        font=SUBTITLE_FONT,
        fill=TEXT_MAIN,
        align="center",
    )
    draw_text_block(
        frame,
        "Personal / educational project using public market data",
        (110, 1135, WIDTH - 110, 1260),
        font=BODY_FONT,
        fill=TEXT_MUTED,
        align="center",
    )
    draw_text_block(
        frame,
        "GitHub: github.com/kunalsingh-finance/PBOR",
        (60, 1450, WIDTH - 60, 1580),
        font=BODY_BOLD,
        fill=ACCENT,
        align="center",
    )
    draw.text((260, 1730), "PBOR-Lite", font=SMALL_BOLD, fill=TEXT_MAIN)
    draw.text((390, 1730), "|", font=SMALL_BOLD, fill=TEXT_MUTED)
    draw.text((430, 1730), "LinkedIn teaser built from repo assets", font=SMALL_FONT, fill=TEXT_MUTED)
    return frame


def render_close_final(assets: dict[str, Image.Image], progress: float) -> Image.Image:
    frame = Image.new("RGBA", CANVAS, hex_rgba(BACKGROUND, 255))
    add_card(frame, assets["tearsheet"], (170, 120, WIDTH - 170, 620), progress=progress)
    draw = ImageDraw.Draw(frame)
    draw_text_block(
        frame,
        "Python | pandas | SQL",
        (90, 760, WIDTH - 90, 910),
        font=SUBTITLE_FONT,
        fill=TEXT_MAIN,
        align="center",
    )
    draw_text_block(
        frame,
        "SQLite | Streamlit",
        (90, 900, WIDTH - 90, 1010),
        font=SUBTITLE_FONT,
        fill=TEXT_MAIN,
        align="center",
    )
    draw_text_block(
        frame,
        "Personal / educational project using public market data",
        (110, 1135, WIDTH - 110, 1260),
        font=BODY_FONT,
        fill=TEXT_MUTED,
        align="center",
    )
    draw_text_block(
        frame,
        "GitHub: github.com/kunalsingh-finance/PBOR",
        (60, 1450, WIDTH - 60, 1580),
        font=BODY_BOLD,
        fill=ACCENT,
        align="center",
    )
    draw.text((260, 1730), "PBOR-Lite", font=SMALL_BOLD, fill=TEXT_MAIN)
    draw.text((390, 1730), "|", font=SMALL_BOLD, fill=TEXT_MUTED)
    draw.text((430, 1730), "LinkedIn teaser built from repo assets", font=SMALL_FONT, fill=TEXT_MUTED)
    return frame


def build_scenes() -> list[Scene]:
    return [
        Scene(duration=4.0, renderer=render_hook),
        Scene(duration=4.5, renderer=render_overview),
        Scene(duration=4.5, renderer=render_workflow),
        Scene(duration=4.0, renderer=render_why),
        Scene(duration=4.0, renderer=render_close_final),
    ]


def load_assets() -> dict[str, Image.Image]:
    return {
        "tearsheet": Image.open(TEARSHEET_PATH).convert("RGBA"),
        "controls": Image.open(CONTROLS_PATH).convert("RGBA"),
    }


def build_cover(assets: dict[str, Image.Image]) -> Image.Image:
    return render_hook(assets, progress=0.35)


def caption_text() -> str:
    return textwrap.dedent(
        """
        PBOR-Lite is a personal/educational project built to better understand the control layer around performance reporting.

        It uses public market data plus bundled sample inputs to simulate a PBOR-style month-end reporting workflow: ingest inputs, calculate returns, run attribution, apply QA/reconciliation checks, and generate reporting outputs.

        Built with Python, pandas, SQL, SQLite, and Streamlit.
        GitHub: https://github.com/kunalsingh-finance/PBOR
        """
    ).strip() + "\n"


def iter_frames(assets: dict[str, Image.Image], scenes: list[Scene]) -> tuple[int, list[Image.Image]]:
    total_frames = 0
    preview_frames: list[Image.Image] = []
    for scene in scenes:
        scene_frames = int(round(scene.duration * FPS))
        for index in range(scene_frames):
            progress = 0.0 if scene_frames <= 1 else index / (scene_frames - 1)
            frame = scene.renderer(assets, progress).convert("RGB")
            total_frames += 1
            if total_frames <= 2:
                preview_frames.append(frame.copy())
            yield frame


def encode_video(assets: dict[str, Image.Image], scenes: list[Scene]) -> tuple[int, float]:
    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
    command = [
        ffmpeg_exe,
        "-y",
        "-f",
        "rawvideo",
        "-vcodec",
        "rawvideo",
        "-s",
        f"{WIDTH}x{HEIGHT}",
        "-pix_fmt",
        "rgb24",
        "-r",
        str(FPS),
        "-i",
        "-",
        "-an",
        "-c:v",
        "libx264",
        "-pix_fmt",
        "yuv420p",
        "-preset",
        "medium",
        "-crf",
        "18",
        "-movflags",
        "+faststart",
        str(OUTPUT_VIDEO),
    ]
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    frame_count = 0
    try:
        for frame in iter_frames(assets, scenes):
            assert process.stdin is not None
            process.stdin.write(frame.tobytes())
            frame_count += 1
    finally:
        if process.stdin is not None:
            process.stdin.close()
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise RuntimeError(
            f"ffmpeg encoding failed with code {process.returncode}\nSTDOUT:\n{stdout.decode(errors='ignore')}\nSTDERR:\n{stderr.decode(errors='ignore')}"
        )
    seconds = frame_count / FPS
    return frame_count, seconds


def verify_video() -> tuple[int, float]:
    frame_count, seconds = imageio_ffmpeg.count_frames_and_secs(str(OUTPUT_VIDEO))
    return int(frame_count), float(seconds)


def main() -> None:
    assets = load_assets()
    scenes = build_scenes()

    cover = build_cover(assets)
    cover.save(OUTPUT_COVER, format="PNG")

    OUTPUT_CAPTION.write_text(caption_text(), encoding="utf-8")

    planned_frames, planned_seconds = encode_video(assets, scenes)
    actual_frames, actual_seconds = verify_video()

    print(f"Created {OUTPUT_VIDEO}")
    print(f"Created {OUTPUT_COVER}")
    print(f"Created {OUTPUT_CAPTION}")
    print(f"Planned duration: {planned_seconds:.2f}s at {WIDTH}x{HEIGHT}, {FPS} fps ({planned_frames} frames)")
    print(f"Verified duration: {actual_seconds:.2f}s ({actual_frames} frames)")


if __name__ == "__main__":
    main()
