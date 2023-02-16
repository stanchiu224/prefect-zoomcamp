from prefect.filesystems import GitHub

# alternative to creating GitHub block in the UI

gh_block = GitHub(
    name="de-zoom", repository="https://github.com/discdiver/prefect-zoomcamp"
)

gh_block.save("de-zoom", overwrite=True)
