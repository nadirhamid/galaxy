package main

import (
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"strconv"
	"syscall"

	docker "github.com/fsouza/go-dockerclient"

	"github.com/kataras/iris"
	"github.com/litl/galaxy/commander"
	"github.com/litl/galaxy/config"
	"github.com/litl/galaxy/discovery"
	"github.com/litl/galaxy/log"
	"github.com/litl/galaxy/runtime"
	"github.com/litl/galaxy/utils"
)


type AppRequest struct {
	Name string `json: "name"`
}
type AppRunRequest struct {
	Args[] string `json: "args"`
}
type AppConfigPostRequest struct {
	Args[] string `json: "args"`
}
type AppConfigResponse struct {
	Env map[string]string `json: "env"`
}
type AppConfigKeyResponse struct {
	Key string `json: "key"`
	Value string `json: "value"`
}
type AppRuntimePool struct {
	Processes string `json: "processes"`
	CPUShares string `json: "cpuShares"`
	Memory string `json: "memory"`
	VirtualHost string `json: "virtualHost"`
	Port string `json: "port"`
	MaintenanceMode string `json: "maintenanceMode"`
        Ps int `json: "ps"`
}
type AppRuntimeResponse struct {
    Pools[] AppRuntimePool `json: "pools"`
}

func AppsGet(ctx*iris.Context){
     apps, err := configStore.ListApps(env)
     if err != nil {
	 ctx.HTML(iris.StatusInternalServerError,"")
     }
     ctx.JSON(iris.StatusOK, apps)
}
func AppGet(ctx*iris.Context) {
    app, err := configStore.GetApp(ctx.Param("name"), env)
    if err != nil {
	 ctx.HTML(iris.StatusInternalServerError,"")
    }
    ctx.JSON(iris.StatusOK, app)
}
func AppCreate(ctx*iris.Context) {
     ensureEnv()
     appRequest := AppRequest{}
     if err := ctx.ReadJSON(&appRequest); err != nil {
	  ctx.HTML(iris.StatusBadRequest,"")
    }
    if err := commander.AppCreate( configStore, appRequest.Name, env ); err != nil {
	  ctx.HTML(iris.StatusInternalServerError,"")
    }
    ctx.HTML(iris.StatusOK, "")
}
func AppDelete(ctx*iris.Context) {
     ensureEnv()
    if err := commander.AppDelete( configStore, ctx.Param("name"), env ); err != nil { 
	  ctx.HTML(iris.StatusInternalServerError,"")
    }
    ctx.HTML(iris.StatusOK, "")
}
func AppAssign(ctx*iris.Context) {
     ensureEnv()
     ensurePool()
     if err := commander.AppDelete( configStore, ctx.Param("name"), env ); err != nil { 
	  ctx.HTML(iris.StatusInternalServerError,"")
	}

    ctx.HTML(iris.StatusOK, "")
}
func AppDeploy(ctx*iris.Context) {
     ensureEnv()
     appRequest := AppRequest{}
     if err := ctx.ReadJSON(&appRequest); err != nil {
	  ctx.HTML(iris.StatusBadRequest,"")
    }
    err := commander.AppDeploy( configStore, serviceRuntime, ctx.Param("name"), appRequest.Name, env )
    if err != nil {
	  ctx.HTML(iris.StatusInternalServerError,"")
    }
    ctx.HTML(iris.StatusOK, "")
}
func AppRun(ctx*iris.Context) {
    ensureEnv()
    appRunRequest := AppRunRequest{}
    if err := ctx.ReadJSON( &appRunRequest ); err != nil {    
	  ctx.HTML(iris.StatusBadRequest,"")
    }
    err := commander.AppRun( configStore, serviceRuntime, ctx.Param("name"),  env, appRunRequest.Args )
    if err != nil {
	  ctx.HTML(iris.StatusInternalServerError,"")
    }
    ctx.HTML(iris.StatusOK, "")
}
func AppStop(ctx*iris.Context) {
    ensureEnv()
    if err := serviceRuntime.StopAllMatching( ctx.Param("name") ); err != nil {
	  ctx.HTML(iris.StatusInternalServerError,"")
    }
    ctx.HTML(iris.StatusOK, "")
}
// NOT IMPLEMENTED
func AppShell(ctx*iris.Context) {
}

func AppUnassign(ctx*iris.Context) {
   ensureEnv()
   ensurePool()
   if err := commander.AppUnassign(configStore, ctx.Param("name"), env, pool ); err != nil {
	  ctx.HTML(iris.StatusInternalServerError,"")
   }
   ctx.HTML(iris.StatusOK, "")
}

// NOT IMPLEMENTED
func AppStart(ctx*iris.Context) {
}


func AppRestart(ctx*iris.Context) {
   ensureEnv()
   if err := commander.AppRestart(configStore, ctx.Param("name"), env  ); err != nil {
	  ctx.HTML(iris.StatusInternalServerError,"")
   }
   ctx.HTML(iris.StatusOK, "")
}


func AppConfig(ctx*iris.Context) {
    ensureEnv()
    app, err :=  configStore.GetApp(ctx.Param("name"), env)
    if err != nil {
	  ctx.HTML(iris.StatusInternalServerError,"")
    }
    appConfigResponse := &AppConfigResponse{
	  Env: app.Env() }
   ctx.JSON(iris.StatusOK, appConfigResponse)
}
func AppConfigGet(ctx*iris.Context) {
    ensureEnv()
    app, err :=  configStore.GetApp(ctx.Param("name"), env)
    if err != nil {
	  ctx.HTML(iris.StatusInternalServerError,"")
    }
    param := ctx.Param("param")
    appConfigKeyResponse := &AppConfigKeyResponse{
	 Key: param,
	 Value: app.EnvGet(param)}
   ctx.JSON(iris.StatusOK, appConfigKeyResponse)
}
func AppConfigSet(ctx*iris.Context) {
   ensureEnv()
   appConfigSetRequest:= AppConfigPostRequest{}
   if err := ctx.ReadJSON(&appConfigSetRequest); err != nil {
	 ctx.HTML(iris.StatusBadRequest, "")
   }
   err := commander.ConfigSet( configStore, ctx.Param("name"), env, appConfigSetRequest.Args ) 
   if err != nil {
	 ctx.HTML(iris.StatusInternalServerError, "")
   }
   ctx.HTML(iris.StatusOK, "")
}
func AppConfigUnset(ctx*iris.Context) {
   ensureEnv()
   appConfigUnsetRequest:= AppConfigPostRequest{}
   if err := ctx.ReadJSON(&appConfigUnsetRequest); err != nil {
	 ctx.HTML(iris.StatusBadRequest, "")
   }
   err := commander.ConfigSet( configStore, ctx.Param("name"), env, appConfigUnsetRequest.Args ) 
   if err != nil {
	 ctx.HTML(iris.StatusInternalServerError, "")
   }
   ctx.HTML(iris.StatusOK, "")

}
func AppRuntime(ctx*iris.Context) {
    ensureEnv()
    app, err := configStore.GetApp(env, ctx.Param("name"))
   if err != nil {
	 ctx.HTML(iris.StatusInternalServerError, "")
   }
   appRuntimeResponse := AppRuntimeResponse{}
   poolsList := make([]AppRuntimePool, len( app.RuntimePools() ),len( app.RuntimePools() ) )
   appRuntimeResponse.Pools = poolsList

   ctx.JSON(iris.StatusOK,appRuntimeResponse)
}
func AppRuntimeSet(ctx*iris.Context) {
    ensureEnv()
    appRuntimePool := AppRuntimePool{}
    if err := ctx.ReadJSON( &appRuntimePool ); err != nil {
	 ctx.HTML(iris.StatusBadRequest, "")
    }
    updated, err := commander.RuntimeSet(configStore,  ctx.Param("name"), env, pool, commander.RuntimeOptions{
		CPUShares: appRuntimePool.CPUShares,
		Ps: appRuntimePool.Ps,
		Memory: appRuntimePool.Memory,
		VirtualHost: appRuntimePool.VirtualHost,
		Port: appRuntimePool.Port,
		MaintenanceMode: appRuntimePool.MaintenanceMode})
  if err != nil {
	 ctx.HTML(iris.StatusInternalServerError, "")
  }
  if !updated {
	ctx.HTML(iris.StatusConflict, "")
  }
  ctx.HTML(iris.StatusOK, "")
}

//NOT IMPLEMENTED
func AppHosts(ctx*iris.Context) {
}
func initOrDie() {

	if registryURL == "" {
		log.Fatalf("ERROR: Registry URL not specified. Use '-registry redis://127.0.0.1:6379' or set 'GALAXY_REGISTRY_URL'")
	}

	configStore = config.NewStore(config.DefaultTTL, registryURL)

	serviceRuntime = runtime.NewServiceRuntime(configStore, dns, hostIP)

	apps, err := configStore.ListAssignments(env, pool)
	if err != nil {
		log.Fatalf("ERROR: Could not retrieve service configs for /%s/%s: %s", env, pool, err)
	}

	workerChans = make(map[string]chan string)
	for _, app := range apps {
		appCfg, err := configStore.GetApp(app, env)
		if err != nil {
			log.Fatalf("ERROR: Could not retrieve service config for /%s/%s: %s", env, pool, err)
		}

		workerChans[appCfg.Name()] = make(chan string)
	}

	signalsChan = make(chan os.Signal, 1)
	signal.Notify(signalsChan, os.Interrupt, os.Kill, syscall.SIGTERM)
	go deregisterHost(signalsChan)
}

func ensureEnv() {
	envs, err := configStore.ListEnvs()
	if err != nil {
		log.Fatalf("ERROR: Could not check envs: %s", err)
	}

	if strings.TrimSpace(env) == "" {
		log.Fatalf("ERROR: Need an env.  Use '-env <env>'. Existing envs are: %s.", strings.Join(envs, ","))
	}
}

func ensurePool() {

	pools, err := configStore.ListPools(env)
	if err != nil {
		log.Fatalf("ERROR: Could not check pools: %s", err)
	}

	if strings.TrimSpace(pool) == "" {
		log.Fatalf("ERROR: Need a pool.  Use '-pool <pool>'. Existing pools are: %s", strings.Join(pools, ","))
	}
}

func pullImageAsync(appCfg config.App, errChan chan error) {
	// err logged via pullImage
	_, err := pullImage(appCfg)
	if err != nil {
		errChan <- err
		return
	}
	errChan <- nil
}

func pullImage(appCfg config.App) (*docker.Image, error) {
	image, err := serviceRuntime.PullImage(appCfg.Version(), appCfg.VersionID())
	if image == nil || err != nil {
		log.Errorf("ERROR: Could not pull image %s: %s", appCfg.Version(), err)
		return nil, err
	}

	log.Printf("Pulled %s version %s\n", appCfg.Name(), appCfg.Version())
	return image, nil
}

func startService(appCfg config.App, logStatus bool) {

	desired, err := commander.Balanced(configStore, hostIP, appCfg.Name(), env, pool)
	if err != nil {
		log.Errorf("ERROR: Could not determine instance count: %s", err)
		return
	}

	running, err := serviceRuntime.InstanceCount(appCfg.Name(), strconv.FormatInt(appCfg.ID(), 10))
	if err != nil {
		log.Errorf("ERROR: Could not determine running instance count: %s", err)
		return
	}

	for i := 0; i < desired-running; i++ {
		container, err := serviceRuntime.Start(env, pool, appCfg)
		if err != nil {
			log.Errorf("ERROR: Could not start containers: %s", err)
			return
		}

		log.Printf("Started %s version %s as %s\n", appCfg.Name(), appCfg.Version(), container.ID[0:12])

		err = serviceRuntime.StopOldVersion(appCfg, 1)
		if err != nil {
			log.Errorf("ERROR: Could not stop containers: %s", err)
		}
	}

	running, err = serviceRuntime.InstanceCount(appCfg.Name(), strconv.FormatInt(appCfg.ID(), 10))
	if err != nil {
		log.Errorf("ERROR: Could not determine running instance count: %s", err)
		return
	}

	for i := 0; i < running-desired; i++ {
		err := serviceRuntime.Stop(appCfg)
		if err != nil {
			log.Errorf("ERROR: Could not stop container: %s", err)
		}
	}

	err = serviceRuntime.StopOldVersion(appCfg, -1)
	if err != nil {
		log.Errorf("ERROR: Could not stop old containers: %s", err)
	}

	// check the image version, and log any inconsistencies
	inspectImage(appCfg)
}

func heartbeatHost() {
	_, err := configStore.CreatePool(pool, env)
	if err != nil {
		log.Fatalf("ERROR: Unabled to create pool %s: %s", pool, err)
	}

	defer wg.Done()
	for {
		configStore.UpdateHost(env, pool, config.HostInfo{
			HostIP: hostIP,
		})

		time.Sleep(45 * time.Second)
	}
}

func deregisterHost(signals chan os.Signal) {
	<-signals
	configStore.DeleteHost(env, pool, config.HostInfo{
		HostIP: hostIP,
	})
	discovery.Unregister(serviceRuntime, configStore, env, pool, hostIP, shuttleAddr)
	os.Exit(0)
}

func appAssigned(app string) (bool, error) {
	assignments, err := configStore.ListAssignments(env, pool)
	if err != nil {
		return false, err
	}

	if !utils.StringInSlice(app, assignments) {
		return false, nil
	}
	return true, nil
}

// inspectImage checks that the running image matches the config.
// We only use this to print warnings, since we likely need to deploy a new
// config version to fix the inconsistency.
func inspectImage(appCfg config.App) {
	image, err := serviceRuntime.InspectImage(appCfg.Version())
	if err != nil {
		log.Println("error inspecting image", appCfg.Version())
		return
	}

	if utils.StripSHA(image.ID) != appCfg.VersionID() {
		log.Printf("warning: %s image ID does not match config", appCfg.Name())
	}
}

func restartContainers(app string, cmdChan chan string) {
	defer wg.Done()
	logOnce := true

	ticker := time.NewTicker(10 * time.Second)

	for {

		select {

		case cmd := <-cmdChan:

			assigned, err := appAssigned(app)
			if err != nil {
				log.Errorf("ERROR: Error retrieving assignments for %s: %s", app, err)
				if !loop {
					return
				}
				continue
			}

			if !assigned {
				continue
			}

			appCfg, err := configStore.GetApp(app, env)
			if err != nil {
				log.Errorf("ERROR: Error retrieving service config for %s: %s", app, err)
				if !loop {
					return
				}
				continue
			}

			if appCfg.Version() == "" {
				if !loop {
					return
				}
				continue
			}

			if cmd == "deploy" {
				_, err = pullImage(appCfg)
				if err != nil {
					log.Errorf("ERROR: Error pulling image for %s: %s", app, err)
					if !loop {
						return
					}
					continue
				}
				startService(appCfg, logOnce)
			}

			if cmd == "restart" {
				err := serviceRuntime.Stop(appCfg)
				if err != nil {
					log.Errorf("ERROR: Could not stop %s: %s",
						appCfg.Version(), err)
					if !loop {
						return
					}

					startService(appCfg, logOnce)
					continue
				}
			}

			logOnce = false
		case <-ticker.C:

			appCfg, err := configStore.GetApp(app, env)
			if err != nil {
				log.Errorf("ERROR: Error retrieving service config for %s: %s", app, err)
				continue
			}

			assigned, err := appAssigned(app)
			if err != nil {
				log.Errorf("ERROR: Error retrieving service config for %s: %s", app, err)
				if !loop {
					return
				}

				continue
			}

			if appCfg == nil || !assigned {
				log.Errorf("%s no longer exists.  Stopping worker.", app)
				serviceRuntime.StopAllMatching(app)
				delete(workerChans, app)
				return
			}

			if appCfg.Version() == "" {
				continue
			}

			startService(appCfg, logOnce)
		}

		if !loop {
			return
		}

	}
}

func monitorService(changedConfigs chan *config.ConfigChange) {

	for {

		var changedConfig *config.ConfigChange
		select {

		case changedConfig = <-changedConfigs:

			if changedConfig.Error != nil {
				log.Errorf("ERROR: Error watching changes: %s", changedConfig.Error)
				continue
			}

			if changedConfig.AppConfig == nil {
				continue
			}

			assigned, err := appAssigned(changedConfig.AppConfig.Name())
			if err != nil {
				log.Errorf("ERROR: Error retrieving service config for %s: %s", changedConfig.AppConfig.Name(), err)
				if !loop {
					return
				}
				continue
			}

			if !assigned {
				continue
			}

			ch, ok := workerChans[changedConfig.AppConfig.Name()]
			if !ok {
				name := changedConfig.AppConfig.Name()
				ch := make(chan string)
				workerChans[name] = ch
				wg.Add(1)
				go restartContainers(name, ch)
				ch <- "deploy"

				log.Printf("Started new worker for %s\n", name)
				continue
			}

			if changedConfig.Restart {
				log.Printf("Restarting %s", changedConfig.AppConfig.Name())
				ch <- "restart"
			} else {
				ch <- "deploy"
			}
		}
	}

}

var (
	env            string
	pool           string
	registryURL    string
	apiPort        int
	loop           bool
	dns            string
	hostIP            string
	shuttleAddr    string
	debug          bool
	runOnce        bool
	version        bool
	buildVersion   string
	configStore    *config.Store
	serviceRuntime *runtime.ServiceRuntime
	workerChans    map[string]chan string
	wg             sync.WaitGroup
	signalsChan    chan os.Signal
)

func main() {
	//iris.Use(logger.New())
	//iris.Use(recovery.New())

	registryURL=utils.GetEnv("GALAXY_REGISTRY_URL", "redis://127.0.0.1:6379")
	env=utils.GetEnv("GALAXY_ENV", "")
	pool=utils.GetEnv("GALAXY_POOL", "")
        apiPort=utils.GetEnv("GALAXY_API_PORT", "7200")
	dns=""
	hostIP="127.0.0.1"

	initOrDie()

	api := iris.New()
	api.Get("/apps", AppsGet)
	api.Get("/app/:name", AppGet)

	api.Post("/app", AppCreate)
	api.Delete("/app/:name", AppDelete)

	api.Post("/app/:name/assign", AppAssign)
	api.Post("/app/:name/deploy", AppDeploy)
	api.Post("/app/:name/restart", AppRestart)
	api.Post("/app/:name/run", AppRun)
	api.Post("/app/:name/stop", AppStop)
	api.Post("/app/:name/shell", AppShell)
	api.Post("/app/:name/unassign", AppUnassign)
	api.Post("/app/:name/run", AppRun)


	api.Get("/app/:name/config", AppConfig)
	api.Get("/app/:name/config/:param", AppConfigGet)

	api.Post("/app/:name/config", AppConfigSet)
	api.Delete("/app/:name/config/:param", AppConfigUnset)

	api.Get("/app/:name/runtime", AppRuntime)
	api.Post("/app/:name/runtime/:param", AppRuntimeSet)

	api.Get("/app/:name/hosts", AppHosts)
	api.Listen(":"+apiPort)
}
