# 📹 FinanceLake Demo Videos

This directory contains demonstration videos for the FinanceLake project.

## Current Videos

- `demo.mp4` - Main project demonstration video

## Adding Videos

To add a demo video to this project:

1. **Copy your video file** to this directory:
   ```bash
   cp /path/to/your/video.mp4 videos/
   ```

2. **Rename if needed** to follow the naming convention:
   ```bash
   mv videos/your-video.mp4 videos/financelake-demo.mp4
   ```

3. **Update the main README.md** if you change the filename

## Video Specifications

- **Format**: MP4 (recommended for web compatibility)
- **Resolution**: 1080p or 720p
- **Codec**: H.264
- **Size**: Keep under 100MB for GitHub compatibility

## Alternative Storage

If your video is too large (>100MB), consider:

1. **Git LFS**: Use Git Large File Storage
   ```bash
   git lfs install
   git lfs track "videos/*.mp4"
   ```

2. **External Hosting**: Upload to YouTube/Vimeo and link in README

3. **Compression**: Reduce file size
   ```bash
   ffmpeg -i input.mp4 -vf scale=1280:720 -crf 28 output.mp4
   ```

## Usage

Videos in this directory are referenced in the main project README.md and can be viewed directly on GitHub or played locally with any video player.
