FROM ipfs/go-ipfs:latest

# Copy startup script
COPY start-ipfs.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/start-ipfs.sh

# Use the script as entrypoint
ENTRYPOINT ["/usr/local/bin/start-ipfs.sh"]