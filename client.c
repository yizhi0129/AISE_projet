// client.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

#define SERVER_PORT 6379
#define BUFFER_SIZE 1024

int main() {
    int sock;
    struct sockaddr_in serverAddr;
    char buffer[BUFFER_SIZE];
    int bytesSent, bytesReceived;

    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("Socket creation failed");
        exit(EXIT_FAILURE);
    }

    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(SERVER_PORT);
    serverAddr.sin_addr.s_addr = INADDR_ANY;

    if (connect(sock, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        perror("Connection failed");
        close(sock);
        exit(EXIT_FAILURE);
    }

    printf("Connected to server.\n");

    while (1) {
        printf("Enter command: ");
        fgets(buffer, BUFFER_SIZE, stdin);
        buffer[strcspn(buffer, "\n")] = 0; // Remove trailing newline

        // Check if the command is QUIT
        if (strcmp(buffer, "QUIT") == 0) {
            printf("Disconnecting from server.\n");
            close(sock);
            exit(EXIT_SUCCESS);
        }

        bytesSent = send(sock, buffer, strlen(buffer), 0);
        if (bytesSent < 0) {
            perror("Failed to send command");
            break;
        }

        bytesReceived = recv(sock, buffer, BUFFER_SIZE - 1, 0);
        if (bytesReceived < 0) {
            perror("Failed to receive response");
            break;
        }
        buffer[bytesReceived] = '\0';

        printf("Server response: %s\n", buffer);
    }

    close(sock);
    return 0;
}
