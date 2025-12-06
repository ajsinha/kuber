#!/bin/bash
# ==============================================================================
# Kuber Distributed Cache - Build Script
# Copyright © 2025-2030, All Rights Reserved
# Ashutosh Sinha | Email: ajsinha@gmail.com
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION="1.2.8"

echo "=============================================="
echo "  Kuber Distributed Cache - Build Script"
echo "  Version: $VERSION"
echo "=============================================="

# Build Java modules
build_java() {
    echo ""
    echo "Building Java modules..."
    cd "$SCRIPT_DIR"
    mvn clean package -DskipTests
    echo "Java build complete!"
}

# Build Python package
build_python() {
    echo ""
    echo "Building Python package..."
    cd "$SCRIPT_DIR/kuber-client-python"
    
    if command -v python3 &> /dev/null; then
        python3 -m pip install --upgrade build 2>/dev/null || true
        python3 -m build 2>/dev/null || echo "Python build skipped (build module not available)"
    else
        echo "Python 3 not found, skipping Python build"
    fi
    
    cd "$SCRIPT_DIR"
    echo "Python build complete!"
}

# Run tests
run_tests() {
    echo ""
    echo "Running tests..."
    cd "$SCRIPT_DIR"
    mvn test
    echo "Tests complete!"
}

# Create distribution archive
create_dist() {
    echo ""
    echo "Creating distribution archive..."
    
    DIST_DIR="$SCRIPT_DIR/dist"
    DIST_NAME="kuber-$VERSION"
    
    rm -rf "$DIST_DIR"
    mkdir -p "$DIST_DIR/$DIST_NAME"
    
    # Copy server JAR
    cp "$SCRIPT_DIR/kuber-server/target/kuber-server-1.2.8-SNAPSHOT.jar" \
       "$DIST_DIR/$DIST_NAME/kuber-server.jar" 2>/dev/null || echo "Server JAR not found"
    
    # Copy client JARs
    mkdir -p "$DIST_DIR/$DIST_NAME/clients/java"
    cp "$SCRIPT_DIR/kuber-client-java/target/kuber-client-java-1.2.8-SNAPSHOT.jar" \
       "$DIST_DIR/$DIST_NAME/clients/java/" 2>/dev/null || echo "Java client JAR not found"
    
    # Copy Python client
    mkdir -p "$DIST_DIR/$DIST_NAME/clients/python"
    cp -r "$SCRIPT_DIR/kuber-client-python/kuber" "$DIST_DIR/$DIST_NAME/clients/python/"
    cp "$SCRIPT_DIR/kuber-client-python/setup.py" "$DIST_DIR/$DIST_NAME/clients/python/"
    cp "$SCRIPT_DIR/kuber-client-python/README.md" "$DIST_DIR/$DIST_NAME/clients/python/"
    
    # Copy configuration
    mkdir -p "$DIST_DIR/$DIST_NAME/config"
    cp "$SCRIPT_DIR/kuber-server/src/main/resources/application.yml" \
       "$DIST_DIR/$DIST_NAME/config/" 2>/dev/null || echo "Config not found"
    
    # Copy documentation
    cp "$SCRIPT_DIR/README.md" "$DIST_DIR/$DIST_NAME/"
    cp "$SCRIPT_DIR/LICENSE" "$DIST_DIR/$DIST_NAME/" 2>/dev/null || echo "LICENSE not found"
    
    # Create startup scripts
    cat > "$DIST_DIR/$DIST_NAME/start.sh" << 'EOF'
#!/bin/bash
java -jar kuber-server.jar "$@"
EOF
    chmod +x "$DIST_DIR/$DIST_NAME/start.sh"
    
    cat > "$DIST_DIR/$DIST_NAME/start.bat" << 'EOF'
@echo off
java -jar kuber-server.jar %*
EOF
    
    # Create archive
    cd "$DIST_DIR"
    tar -czvf "$DIST_NAME.tar.gz" "$DIST_NAME"
    zip -r "$DIST_NAME.zip" "$DIST_NAME"
    
    echo ""
    echo "Distribution archives created:"
    echo "  - $DIST_DIR/$DIST_NAME.tar.gz"
    echo "  - $DIST_DIR/$DIST_NAME.zip"
}

# Clean build artifacts
clean() {
    echo ""
    echo "Cleaning build artifacts..."
    cd "$SCRIPT_DIR"
    mvn clean
    rm -rf "$SCRIPT_DIR/dist"
    rm -rf "$SCRIPT_DIR/kuber-client-python/dist"
    rm -rf "$SCRIPT_DIR/kuber-client-python/build"
    rm -rf "$SCRIPT_DIR/kuber-client-python/*.egg-info"
    echo "Clean complete!"
}

# Print usage
usage() {
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  build     Build all modules (default)"
    echo "  test      Run tests"
    echo "  dist      Create distribution archive"
    echo "  clean     Clean build artifacts"
    echo "  all       Build, test, and create distribution"
    echo ""
}

# Main
case "${1:-build}" in
    build)
        build_java
        build_python
        ;;
    test)
        run_tests
        ;;
    dist)
        build_java
        create_dist
        ;;
    clean)
        clean
        ;;
    all)
        build_java
        build_python
        run_tests
        create_dist
        ;;
    *)
        usage
        exit 1
        ;;
esac

echo ""
echo "Done!"
