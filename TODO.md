1. Add config for default catalog location
2. Add config for default photo search paths
3. Add config for paths to exclude from search
4. Ignore *.lrtpreview in .lrt
5. ~~Be able to update the catalog for moved paths~~
6. Add cronjob for cataloging
7. Add cronjob for verification - short (presence only)/full (with checksum verification)
8. Add GPS files (GPX, etc.)
9. Add XMP files? Changeable, though. Same for PSD files.
10. PDFs?
11. Add removal warning for cloud objects in Archive storage class
12. Upload test data to a public bucket for other developers/users
13. Metadata collision handling. Should we store separate copies for duplicate files? That's what currently happens. If not, how do we distinguish from a true hash collision - file name, file size, other metadata values?
14. Extend download tool to pull entire directories and download the catalog
15. Upload the metadata catalog?
